use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use eyre::WrapErr;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use url::Url;

/// The Storage trait represents a simple key-value store backend. This could be an
/// in-memory HashMap, an embedded database like LMDB or SQLite, or a more complex system.
pub trait Storage: Clone + Send + Sync + 'static {
    fn insert(&self, key: String, value: Vec<u8>);

    fn remove(&self, key: &str) -> Option<Vec<u8>>;

    fn get(&self, key: &str) -> Option<Vec<u8>>;
}

/// The Peers trait represents a list of peers.
pub trait Peers: Clone + Send + Sync + 'static {
    fn insert(&self, url: String);

    fn contains(&self, url: &str) -> bool;

    fn remove(&self, url: &str) -> bool;

    fn iter(&self) -> impl Iterator<Item = String>;
}

#[derive(Clone)]
struct Node<S: Storage, P: Peers> {
    id: String,
    store: S,
    peers: P,
    client: Client,
}

#[derive(Debug, Serialize)]
struct InsertResponse {
    message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action")]
enum ReplicationAction {
    Insert { key: String, value: Vec<u8> },
    Delete { key: String },
    Peers { peers: Vec<String> },
    InitChunk { pairs: Vec<(String, Vec<u8>)> },
    AddPeer { peer: String },
    RemovePeer { peer: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PeerStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerInfo {
    url: String,
}

impl<S: Storage, P: Peers> Node<S, P> {
    async fn replicate_to_peers(&self, action: ReplicationAction) {
        for peer_url in self.peers.iter() {
            let client = self.client.clone();
            let action = action.clone();

            info!("Replicating to peer: {}", peer_url);

            let result = client
                .post(Url::parse(&format!("{}/replicate", peer_url)).unwrap())
                .json(&action)
                .send()
                .await
                .wrap_err_with(|| format!("Failed replicating to peer {peer_url}"));

            if let Err(e) = result {
                error!("{:?}", e);
            }
        }
    }

    async fn insert(&self, key: String, value: Bytes) {
        self.store.insert(key.clone(), value.clone().into());

        self.replicate_to_peers(ReplicationAction::Insert {
            key,
            value: value.into(),
        })
        .await;
    }

    async fn delete(&self, key: &str) {
        self.store.remove(key);

        self.replicate_to_peers(ReplicationAction::Delete {
            key: key.to_string(),
        })
        .await;
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.get(key)
    }
}

async fn insert(
    State(state): State<Node<impl Storage, impl Peers>>,
    Path(key): Path<String>,
    value: Bytes,
) -> impl IntoResponse {
    info!("POST /insert");

    state.insert(key, value).await;

    (
        StatusCode::OK,
        Json(InsertResponse {
            message: "OK".to_string(),
        }),
    )
}

async fn delete_key(
    Path(key): Path<String>,
    State(state): State<Node<impl Storage, impl Peers>>,
) -> impl IntoResponse {
    info!("DELETE /delete/{}", key);

    state.delete(&key).await;

    StatusCode::NO_CONTENT
}

async fn get_key(
    Path(key): Path<String>,
    State(state): State<Node<impl Storage, impl Peers>>,
) -> impl IntoResponse {
    info!("GET /get/{}", key);
    match state.get(&key).await {
        Some(value) => (StatusCode::OK, value.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn replicate(
    State(state): State<Node<impl Storage, impl Peers>>,
    Json(action): Json<ReplicationAction>,
) -> impl IntoResponse {
    info!("Received replication action: {:?}", action);
    match action {
        ReplicationAction::Insert { key, value } => {
            state.store.insert(key, value);
        }
        ReplicationAction::Delete { key } => {
            state.store.remove(&key);
        }
        ReplicationAction::InitChunk { pairs } => {
            for (key, value) in pairs {
                state.store.insert(key, value);
            }
        }
        ReplicationAction::Peers { peers } => {
            for peer in peers {
                if peer == state.id {
                    continue;
                }
                if !state.peers.contains(peer.as_str()) {
                    state.peers.insert(peer.clone());
                    let state = state.clone();
                    state
                        .replicate_to_peers(ReplicationAction::AddPeer { peer })
                        .await;
                }
            }
        }
        ReplicationAction::AddPeer { peer } => {
            if peer != state.id {
                state.peers.insert(peer);
            }
        }
        ReplicationAction::RemovePeer { peer } => {
            if state.peers.remove(&peer) {
                state
                    .replicate_to_peers(ReplicationAction::RemovePeer { peer })
                    .await;
            }
        }
    }
    StatusCode::OK
}

async fn add_peer(
    State(state): State<Node<impl Storage, impl Peers>>,
    Json(info): Json<PeerInfo>,
) -> impl IntoResponse {
    if !state.peers.contains(info.url.as_str()) {
        state.peers.insert(info.url.to_string());
        info!("Added peer: {}", info.url);
    }

    state
        .replicate_to_peers(ReplicationAction::Peers {
            peers: state
                .peers
                .iter()
                .chain(std::iter::once(state.id.clone()))
                .collect(),
        })
        .await;

    StatusCode::CREATED
}

async fn remove_peer(
    State(state): State<Node<impl Storage, impl Peers>>,
    Path(url): Path<String>,
) -> impl IntoResponse {
    state.peers.remove(&url);

    state
        .replicate_to_peers(ReplicationAction::RemovePeer {
            peer: url.to_string(),
        })
        .await;

    info!("Removed peer: {}", url);
    StatusCode::NO_CONTENT
}

async fn list_peers(State(state): State<Node<impl Storage, impl Peers>>) -> impl IntoResponse {
    info!("GET /peers");
    let peers = state.peers.iter().collect::<Vec<_>>();
    Json(peers)
}

#[derive(Clone)]
struct InMemoryStorage {
    data: Arc<papaya::HashMap<String, Vec<u8>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(papaya::HashMap::new()),
        }
    }
}

impl Storage for InMemoryStorage {
    fn insert(&self, key: String, value: Vec<u8>) {
        self.data.pin().insert(key, value);
    }

    fn remove(&self, key: &str) -> Option<Vec<u8>> {
        self.data.pin().remove(key).cloned()
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.pin().get(key).cloned()
    }
}

#[derive(Clone)]
struct InMemoryPeers {
    data: Arc<papaya::HashSet<String>>,
}

impl InMemoryPeers {
    pub fn new() -> Self {
        Self {
            data: Arc::new(papaya::HashSet::new()),
        }
    }
}

impl Peers for InMemoryPeers {
    fn insert(&self, url: String) {
        self.data.pin().insert(url);
    }

    fn contains(&self, url: &str) -> bool {
        self.data.pin().contains(url)
    }

    fn remove(&self, url: &str) -> bool {
        self.data.pin().remove(url)
    }

    fn iter(&self) -> impl Iterator<Item = String> {
        self.data
            .pin()
            .into_iter()
            .cloned()
            .collect::<Vec<String>>()
            .into_iter()
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    #[cfg(debug_assertions)]
    let listener = {
        let mut i = 0;
        loop {
            let addr = format!("127.0.0.1:3{i:03}");
            match TcpListener::bind(&addr).await {
                Ok(listener) => {
                    info!("Listening on http://{}", listener.local_addr()?);
                    break listener;
                }
                Err(_) => {
                    i += 1;
                }
            }
        }
    };

    #[cfg(not(debug_assertions))]
    let listener = TcpListener::bind("127.0.0.1:3000").await?;

    let app_state = Node {
        id: format!("http://{}", listener.local_addr()?),
        store: InMemoryStorage::new(),
        peers: InMemoryPeers::new(),
        client: Client::new(),
    };

    let router = Router::new()
        .route("/kv/{key}", get(get_key).delete(delete_key).post(insert))
        .route("/replicate", post(replicate))
        .route("/peers", post(add_peer).get(list_peers))
        .route("/peers/{url}", delete(remove_peer))
        .with_state(app_state);

    axum::serve(listener, router).await?;

    Ok(())
}
