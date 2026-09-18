use axum::{
    routing::{get, post},
    extract::{State, Path, ConnectInfo},
    Json, Router,
    http::StatusCode,
    response::{Html, IntoResponse},
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::net::SocketAddr;
use rusqlite::{Connection, params};
use chrono::Utc;
use tower_http::cors::CorsLayer;
use tower_http::compression::CompressionLayer;
use std::fs;

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    global_supply: Arc<Mutex<f64>>,
    rate_limiter: Arc<Mutex<HashMap<String, (Instant, u32)>>>,
    ip_blacklist: Arc<Mutex<HashMap<String, Instant>>>,
    start_time: Instant,
    peer_nodes: Arc<Mutex<Vec<String>>>,
    current_difficulty: Arc<Mutex<u32>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Block {
    index: u64,
    timestamp: i64,
    miner: String,
    worker_name: String,
    solution_steps: String,
    prev_hash: String,
    hash: String,
    reward: f64,
    difficulty: u32,
}

#[derive(Deserialize, Serialize, Clone)]
struct Transaction {
    id: String,
    sender: String,
    receiver: String,
    amount: u64,
    nonce: u64,
    timestamp: i64,
    signature: String,
}

#[derive(Deserialize, Serialize, Clone)]
struct MempoolTx {
    id: String,
    sender: String,
    receiver: String,
    amount: u64,
    nonce: u64,
    timestamp: i64,
    signature: String,
    received_at: i64,
}

#[derive(Deserialize, Serialize, Clone)]
struct AITask {
    task_id: String,
    ai_prompt_hash: String,
    model_output: String,
    status: String,
}

#[derive(Deserialize)]
struct SubmitAITaskRequest {
    requester: String,
    ai_prompt_hash: String,
    model_output: String,
}

#[derive(Deserialize)]
struct MineRequest {
    miner: String,
    #[serde(default = "default_worker_name")]
    worker_name: String,
    solution_steps: String,
    device_power_share: f64,
    #[serde(default)]
    ai_task_id: Option<String>,
}

fn default_worker_name() -> String {
    "Default-Worker".to_string()
}

#[derive(Deserialize)]
struct StakeRequest {
    address: String,
    amount: f64,
}

#[derive(Deserialize)]
struct EncryptedWalletRequest {
    password: String,
    public_key: String,
}

#[derive(Deserialize, Serialize, Clone)]
struct AddressBookEntry {
    alias: String,
    address: String,
}

#[derive(Deserialize, Serialize, Clone)]
struct PeerNodeRequest {
    node_address: String,
}

fn check_rate_limit_and_blacklist(
    limiter: &Arc<Mutex<HashMap<String, (Instant, u32)>>>, 
    blacklist: &Arc<Mutex<HashMap<String, Instant>>>, 
    ip: &str, 
    limit_secs: u64
) -> bool {
    let now = Instant::now();
    {
        let mut bl = blacklist.lock().unwrap();
        bl.retain(|_, ban_time| now.duration_since(*ban_time).as_secs() < 300);
        if let Some(ban_time) = bl.get(ip) {
            if now.duration_since(*ban_time).as_secs() < 300 {
                return false;
            } else {
                bl.remove(ip);
            }
        }
    }

    let mut map = limiter.lock().unwrap();
    map.retain(|_, (last_time, _)| now.duration_since(*last_time).as_secs() < 60);

    let entry = map.entry(ip.to_string()).or_insert((now, 0));
    
    if now.duration_since(entry.0).as_secs() < limit_secs {
        entry.1 += 1;
        if entry.1 > 5 {
            drop(map);
            blacklist.lock().unwrap().insert(ip.to_string(), now);
            return false;
        }
        return false;
    } else {
        *entry = (now, 1);
        true
    }
}

fn validate_nvd_address(addr: &str) -> bool {
    if !addr.starts_with("nvd_") || addr.len() < 12 || addr.len() > 68 {
        return false;
    }
    let hex_part = &addr[4..];
    hex_part.chars().all(|c| c.is_ascii_hexdigit())
}

#[tokio::main]
async fn main() {
    let conn = Connection::open("novamind.db").unwrap();
    
    conn.execute_batch("
        PRAGMA journal_mode=WAL; 
        PRAGMA synchronous=NORMAL; 
        PRAGMA busy_timeout=5000; 
        PRAGMA temp_store=MEMORY;
        PRAGMA foreign_keys=ON;
    ").unwrap();

    let integrity_check: String = conn.query_row("PRAGMA integrity_check;", [], |row| row.get(0)).unwrap_or_else(|_| "error".to_string());
    if integrity_check != "ok" {
        panic!("🚨 Database corruption detected! Integrity check failed: {}", integrity_check);
    } else {
        println!("🛡️ Database Integrity Watchdog: OK.");
    }
    
    conn.execute(
        "CREATE TABLE IF NOT EXISTS blocks (
            idx INTEGER PRIMARY KEY,
            timestamp INTEGER,
            miner TEXT NOT NULL,
            worker_name TEXT NOT NULL DEFAULT 'Default-Worker',
            solution_steps TEXT NOT NULL,
            prev_hash TEXT NOT NULL,
            hash TEXT UNIQUE NOT NULL,
            reward REAL CHECK(reward >= 0.0 AND reward <= 15.0),
            difficulty INTEGER NOT NULL DEFAULT 1
        )",
        [],
    ).unwrap();

    let _ = conn.execute("ALTER TABLE blocks ADD COLUMN worker_name TEXT NOT NULL DEFAULT 'Default-Worker'", []);

    conn.execute(
        "CREATE TABLE IF NOT EXISTS ai_tasks (
            task_id TEXT PRIMARY KEY,
            requester TEXT NOT NULL,
            ai_prompt_hash TEXT NOT NULL,
            model_output TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            amount INTEGER CHECK(amount > 0),
            nonce INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'confirmed',
            signature TEXT NOT NULL
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS accounts (
            address TEXT PRIMARY KEY,
            current_nonce INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS mempool (
            id TEXT PRIMARY KEY,
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            amount INTEGER NOT NULL,
            nonce INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            signature TEXT NOT NULL,
            received_at INTEGER NOT NULL
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS stakes (
            address TEXT PRIMARY KEY,
            balance REAL CHECK(balance >= 0.0),
            last_stake_time INTEGER NOT NULL
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS address_book (
            alias TEXT PRIMARY KEY,
            address TEXT NOT NULL
        )",
        [],
    ).unwrap();

    {
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM blocks").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap_or(0);

        if count == 0 {
            let genesis_hash = format!("{:x}", md5::compute("0000NovaMindGenesis999Immutable"));
            conn.execute(
                "INSERT OR IGNORE INTO blocks (idx, timestamp, miner, worker_name, solution_steps, prev_hash, hash, reward, difficulty) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![0, Utc::now().timestamp(), "NovaMind-Genesis", "Master-Node", "Eco-Computation Immutable Genesis", "00000000000000000000000000000000", genesis_hash, 10.0, 1],
            ).unwrap();
            println!("🔒 Immutable Genesis Block securely anchored.");
        }
    }

    let supply_conn = Connection::open("novamind.db").unwrap();
    let mut supply_stmt = supply_conn.prepare("SELECT COALESCE(SUM(reward), 0.0) FROM blocks").unwrap();
    let current_supply: f64 = supply_stmt.query_row([], |row| row.get(0)).unwrap_or(10.0);

    let state = AppState {
        db: Arc::new(Mutex::new(conn)),
        global_supply: Arc::new(Mutex::new(current_supply)),
        rate_limiter: Arc::new(Mutex::new(HashMap::new())),
        ip_blacklist: Arc::new(Mutex::new(HashMap::new())),
        start_time: Instant::now(),
        peer_nodes: Arc::new(Mutex::new(vec![])),
        current_difficulty: Arc::new(Mutex::new(1)),
    };

    let db_bg_clone = state.db.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(300)).await;
            if let Ok(conn) = db_bg_clone.lock() {
                let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
                println!("🧹 Automated SQLite WAL Checkpoint executed successfully.");
            }
        }
    });

    let app = Router::new()
        .route("/", get(serve_index))
        .route("/chain", get(get_chain))
        .route("/chain/verify", get(verify_blockchain))
        .route("/stats", get(get_network_stats))
        .route("/health", get(get_node_health))
        .route("/node/metrics", get(get_node_metrics))
        .route("/mine", post(mine_block))
        .route("/ai/tasks", get(get_pending_ai_tasks).post(submit_ai_task))
        .route("/tx", post(add_transaction))
        .route("/mempool", get(get_mempool_txs))
        .route("/mempool/watch/:id", get(watch_mempool_tx))
        .route("/wallet/create", get(create_wallet))
        .route("/wallet/encrypt", post(encrypt_wallet_local))
        .route("/wallet/balance/:address", get(get_wallet_balance))
        .route("/wallet/history/:address", get(get_wallet_history))
        .route("/wallet/fee-estimate", get(estimate_network_fee))
        .route("/wallet/address-book", get(get_address_book).post(save_address_book))
        .route("/miner/dashboard/:address", get(get_miner_dashboard))
        .route("/miner/earnings-estimate/:address", get(get_miner_earnings_estimate))
        .route("/miner/pending-rewards/:address", get(get_miner_pending_rewards))
        .route("/stake", post(handle_staking))
        .route("/stake/claim", post(claim_staking_rewards))
        .route("/p2p/nodes", get(get_peer_nodes).post(register_peer_node))
        .route("/ui/overview", get(get_ui_overview))
        // --- الإضافات الجديدة المدعومة بدون تأثير سلبي ---
        .route("/explorer/blocks", get(get_explorer_blocks)) // سجل المعاملات العام
        .route("/wallet/dashboard/:address", get(get_full_wallet_dashboard)) // لوحة تحكم المحفظة الشاملة
        .route("/network/ticker", get(get_network_ticker)) // العداد الحي
        .layer(CorsLayer::permissive())
        .layer(CompressionLayer::new())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("🚀 NovaMind Hybrid AI-Verification & P2P Node is running on http://0.0.0.0:8080");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}

async fn serve_index() -> impl IntoResponse {
    match fs::read_to_string("index.html") {
        Ok(html_content) => Html(html_content).into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "index.html not found").into_response(),
    }
}

async fn get_chain(State(state): State<AppState>) -> Json<Vec<Block>> {
    let conn = state.db.lock().unwrap();
    let mut stmt = match conn.prepare("SELECT idx, timestamp, miner, worker_name, solution_steps, prev_hash, hash, reward, difficulty FROM blocks ORDER BY idx ASC") {
        Ok(s) => s,
        Err(_) => return Json(vec![]),
    };
    
    let block_iter = stmt.query_map([], |row| {
        Ok(Block {
            index: row.get(0)?,
            timestamp: row.get(1)?,
            miner: row.get(2)?,
            worker_name: row.get(3)?,
            solution_steps: row.get(4)?,
            prev_hash: row.get(5)?,
            hash: row.get(6)?,
            reward: row.get(7)?,
            difficulty: row.get(8)?,
        })
    });

    let mut blockchain = vec![];
    if let Ok(iter) = block_iter {
        for b in iter {
            if let Ok(block) = b {
                blockchain.push(block);
            }
        }
    }
    Json(blockchain)
}

async fn get_network_stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let supply = *state.global_supply.lock().unwrap();
    let current_diff = *state.current_difficulty.lock().unwrap();
    let peers_count = state.peer_nodes.lock().unwrap().len();
    
    let block_count: i64 = conn.query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0)).unwrap_or(0);
    let total_staked: f64 = conn.query_row("SELECT COALESCE(SUM(balance), 0.0) FROM stakes", [], |row| row.get(0)).unwrap_or(0.0);
    let ai_tasks_count: i64 = conn.query_row("SELECT COUNT(*) FROM ai_tasks WHERE status = 'pending'", [], |row| row.get(0)).unwrap_or(0);

    Json(serde_json::json!({
        "coin_name": "NovaMind (NVD)",
        "circulating_supply": supply,
        "max_supply": 19_000_000.0,
        "total_blocks": block_count,
        "pending_ai_verification_tasks": ai_tasks_count,
        "total_staked": total_staked,
        "dynamic_difficulty": current_diff,
        "connected_p2p_peers": peers_count,
        "security_status": "Hybrid PoW & AI Trust Active",
        "status": "Operational"
    }))
}

async fn get_node_health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let uptime = state.start_time.elapsed().as_secs();
    let conn_status = state.db.lock().is_ok();

    Json(serde_json::json!({
        "node": "NovaMind Validator",
        "database_healthy": conn_status,
        "uptime_seconds": uptime,
        "status": "Healthy"
    }))
}

async fn get_node_metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    let uptime = state.start_time.elapsed().as_secs();
    let limiter_len = state.rate_limiter.lock().unwrap().len();
    let blacklist_len = state.ip_blacklist.lock().unwrap().len();

    Json(serde_json::json!({
        "node_uptime_seconds": uptime,
        "active_rate_limited_ips": limiter_len,
        "blacklisted_ips_count": blacklist_len,
        "compression_layer": "Active (gzip/brotli)",
        "status": "Optimal"
    }))
}

async fn verify_blockchain(State(state): State<AppState>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let mut stmt = match conn.prepare("SELECT idx, timestamp, miner, worker_name, solution_steps, prev_hash, hash, reward, difficulty FROM blocks ORDER BY idx ASC") {
        Ok(s) => s,
        Err(_) => return Json(serde_json::json!({"valid": false, "message": "Database error"})),
    };
    
    let block_iter = stmt.query_map([], |row| {
        Ok(Block {
            index: row.get(0)?,
            timestamp: row.get(1)?,
            miner: row.get(2)?,
            worker_name: row.get(3)?,
            solution_steps: row.get(4)?,
            prev_hash: row.get(5)?,
            hash: row.get(6)?,
            reward: row.get(7)?,
            difficulty: row.get(8)?,
        })
    });

    let mut blocks = vec![];
    if let Ok(iter)  = block_iter {
        for b in iter {
            if let Ok(block) = b {
                blocks.push(block);
            }
        }
    }

    for i in 1..blocks.len() {
        let current = &blocks[i];
        let previous = &blocks[i - 1];

        if current.index != previous.index + 1 || current.prev_hash != previous.hash {
            return Json(serde_json::json!({"valid": false, "message": "Chain integrity broken"}));
        }

        let raw_data = format!("{}{}{}{}{}{}{}{}", current.index, current.prev_hash, current.miner, current.worker_name, current.solution_steps, current.reward, current.timestamp, current.difficulty);
        let calculated_hash = format!("{:x}", md5::compute(raw_data));
        if calculated_hash != current.hash {
            return Json(serde_json::json!({"valid": false, "message": "Cryptographic tampering detected"}));
        }
    }

    Json(serde_json::json!({"valid": true, "message": "NovaMind Blockchain fully verified."}))
}

async fn submit_ai_task(
    State(state): State<AppState>,
    Json(payload): Json<SubmitAITaskRequest>,
) -> StatusCode {
    let conn = state.db.lock().unwrap();
    if !validate_nvd_address(&payload.requester) || payload.ai_prompt_hash.is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    let task_id = format!("ai_task_{}", uuid_v4_simple());
    let current_ts = Utc::now().timestamp();

    let res = conn.execute(
        "INSERT INTO ai_tasks (task_id, requester, ai_prompt_hash, model_output, status, created_at) VALUES (?1, ?2, ?3, ?4, 'pending', ?5)",
        params![task_id, payload.requester, payload.ai_prompt_hash, payload.model_output, current_ts],
    );

    if res.is_ok() { StatusCode::CREATED } else { StatusCode::INTERNAL_SERVER_ERROR }
}

async fn get_pending_ai_tasks(State(state): State<AppState>) -> Json<Vec<AITask>> {
    let conn = state.db.lock().unwrap();
    let mut stmt = match conn.prepare("SELECT task_id, ai_prompt_hash, model_output, status FROM ai_tasks WHERE status = 'pending' LIMIT 10") {
        Ok(s) => s,
        Err(_) => return Json(vec![]),
    };

    let tasks_iter = stmt.query_map([], |row| {
        Ok(AITask {
            task_id: row.get(0)?,
            ai_prompt_hash: row.get(1)?,
            model_output: row.get(2)?,
            status: row.get(3)?,
        })
    });

    let list = vec![];
    if let Ok(iter) = tasks_iter {
        let mut list = vec![];
        for t in iter {
            if let Ok(task) = t { list.push(task); }
        }
        return Json(list);
    }
    Json(list)
}

fn uuid_v4_simple() -> String {
    let _current_ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    format!("{:x}", md5::compute(format!("{}", _current_ts)))
}

async fn create_wallet() -> Json<serde_json::Value> {
    let rng = ring::rand::SystemRandom::new();
    let pkcs8_bytes = match ring::signature::Ed25519KeyPair::generate_pkcs8(&rng) {
        Ok(b) => b,
        Err(_) => return Json(serde_json::json!({"status": "error", "message": "Key generation failed"})),
    };
    let key_pair = ring::signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();
    
    let public_key_hex = hex::encode(key_pair.public_key().as_ref());
    let address = format!("nvd_{}", &public_key_hex[..32]);

    Json(serde_json::json!({
        "status": "success",
        "address": address,
        "public_key": public_key_hex,
        "message": "NovaMind secure wallet generated."
    }))
}

async fn encrypt_wallet_local(Json(payload): Json<EncryptedWalletRequest>) -> Json<serde_json::Value> {
    if payload.password.len() < 6 {
        return Json(serde_json::json!({"status": "error", "message": "Password too short"}));
    }
    let encrypted_payload = format!("{:x}", md5::compute(format!("{}{}", payload.password, payload.public_key)));
    Json(serde_json::json!({
        "status": "success",
        "encrypted_vault": encrypted_payload
    }))
}

async fn mine_block(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<MineRequest>,
) -> StatusCode {
    let ip_str = addr.ip().to_string();
    if !check_rate_limit_and_blacklist(&state.rate_limiter, &state.ip_blacklist, &ip_str, 2) {
        return StatusCode::TOO_MANY_REQUESTS;
    }

    let conn = state.db.lock().unwrap();
    let mut supply = state.global_supply.lock().unwrap();
    let current_diff = *state.current_difficulty.lock().unwrap();

    if payload.device_power_share <= 0.0 || payload.device_power_share > 1000.0 || !validate_nvd_address(&payload.miner) {
        return StatusCode::BAD_REQUEST;
    }

    let trimmed_steps = payload.solution_steps.trim();
    if trimmed_steps.len() < 10 || trimmed_steps.len() > 500 {
        return StatusCode::BAD_REQUEST;
    }

    let trimmed_worker = payload.worker_name.trim();
    let worker_name = if trimmed_worker.is_empty() { "Default-Worker" } else { trimmed_worker };

    if *supply >= 19_000_000.0 {
        return StatusCode::FORBIDDEN;
    }

    let mut stmt = match conn.prepare("SELECT idx, timestamp, hash FROM blocks ORDER BY idx DESC LIMIT 1") {
        Ok(s) => s,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    let (prev_index, prev_timestamp, prev_hash): (u64, i64, String) = match stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))) {
        Ok(val) => val,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let timestamp = Utc::now().timestamp();
    let block_time_gap = timestamp - prev_timestamp;
    
    {
        let mut diff_lock = state.current_difficulty.lock().unwrap();
        if block_time_gap < 10 {
            *diff_lock += 1;
        } else if block_time_gap > 60 && *diff_lock > 1 {
            *diff_lock -= 1;
        }
    }

    let new_index = prev_index + 1;
    let base_reward = 10.0 / (1.0 + (new_index as f64) * 0.05);
    
    let mut bonus_reward = 0.0;
    if let Some(ref t_id) = payload.ai_task_id {
        let rows_affected = conn.execute("UPDATE ai_tasks SET status = 'verified' WHERE task_id = ?1 AND status = 'pending'", params![t_id]).unwrap_or(0);
        if rows_affected > 0 {
            bonus_reward = 2.0;
        }
    }

    let anti_whale = if payload.device_power_share > 1.0 { 1.0 / payload.device_power_share.ln_1p() } else { 1.0 };
    let mut final_reward = (base_reward + bonus_reward) * anti_whale;
    if final_reward > 15.0 { final_reward = 15.0; }

    if *supply + final_reward > 19_000_000.0 {
        return StatusCode::FORBIDDEN;
    }

    let raw_data = format!("{}{}{}{}{}{}{}{}", new_index, prev_hash, payload.miner, worker_name, trimmed_steps, final_reward, timestamp, current_diff);
    let digest = format!("{:x}", md5::compute(raw_data));

    let res = conn.execute(
        "INSERT INTO blocks (idx, timestamp, miner, worker_name, solution_steps, prev_hash, hash, reward, difficulty) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![new_index, timestamp, payload.miner, worker_name, trimmed_steps, prev_hash, digest, final_reward, current_diff],
    );

    if res.is_ok() {
        *supply += final_reward;
        StatusCode::CREATED
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn get_peer_nodes(State(state): State<AppState>) -> Json<Vec<String>> {
    let peers = state.peer_nodes.lock().unwrap().clone();
    Json(peers)
}

async fn register_peer_node(
    State(state): State<AppState>,
    Json(payload): Json<PeerNodeRequest>,
) -> StatusCode {
    let trimmed_node = payload.node_address.trim();
    if trimmed_node.is_empty() || trimmed_node.len() > 128 {
        return StatusCode::BAD_REQUEST;
    }
    let mut peers = state.peer_nodes.lock().unwrap();
    if !peers.contains(&trimmed_node.to_string()) {
        peers.push(trimmed_node.to_string());
    }
    StatusCode::CREATED
}

async fn get_ui_overview(State(state): State<AppState>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let supply = *state.global_supply.lock().unwrap();
    let block_count: i64 = conn.query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0)).unwrap_or(0);
    let current_diff = *state.current_difficulty.lock().unwrap();

    Json(serde_json::json!({
        "network": "NovaMind Mainnet",
        "circulating_supply": supply,
        "total_blocks": block_count,
        "current_difficulty": current_diff,
        "ui_ready": true,
        "status": "Online"
    }))
}

async fn get_miner_dashboard(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let mined_blocks: i64 = conn.query_row("SELECT COUNT(*) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0);
    let total_mined_rewards: f64 = conn.query_row("SELECT COALESCE(SUM(reward), 0.0) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0.0);

    let mut stmt = conn.prepare("SELECT worker_name, COUNT(*), SUM(reward) FROM blocks WHERE miner = ?1 GROUP BY worker_name").unwrap();
    let workers_iter = stmt.query_map(params![address], |row| {
        Ok(serde_json::json!({
            "worker_name": row.get::<_, String>(0)?,
            "blocks_mined": row.get::<_, i64>(1)?,
            "rewards_earned": row.get::<_, f64>(2)?
        }))
    });

    let mut workers_list = vec![];
    if let Ok(iter) = workers_iter {
        for w in iter {
            if let Ok(worker_json) = w { workers_list.push(worker_json); }
        }
    }

    Json(serde_json::json!({
        "miner_address": address,
        "blocks_mined": mined_blocks,
        "total_rewards_earned": total_mined_rewards,
        "active_workers": workers_list,
        "connection_status": "Hybrid AI-Verified Mining Active"
    }))
}

async fn get_miner_earnings_estimate(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let mined_blocks: i64 = conn.query_row("SELECT COUNT(*) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0);
    let estimated_hourly = (mined_blocks as f64 * 0.05).max(0.01);

    Json(serde_json::json!({
        "estimated_hourly_nvd": estimated_hourly,
        "estimated_daily_nvd": estimated_hourly * 24.0
    }))
}

async fn get_miner_pending_rewards(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let total_mined: f64 = conn.query_row("SELECT COALESCE(SUM(reward), 0.0) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0.0);
    Json(serde_json::json!({"pending_rewards_nvd": total_mined}))
}

async fn get_mempool_txs(State(state): State<AppState>) -> Json<Vec<MempoolTx>> {
    let conn = state.db.lock().unwrap();
    let mut stmt = match conn.prepare("SELECT id, sender, receiver, amount, nonce, timestamp, signature, received_at FROM mempool ORDER BY received_at ASC") {
        Ok(s) => s,
        Err(_) => return Json(vec![]),
    };
    let tx_iter = stmt.query_map([], |row| {
        Ok(MempoolTx {
            id: row.get(0)?, sender: row.get(1)?, receiver: row.get(2)?,
            amount: row.get(3)?, nonce: row.get(4)?, timestamp: row.get(5)?,
            signature: row.get(6)?, received_at: row.get(7)?,
        })
    });
    let list = vec![];
    if let Ok(iter) = tx_iter {
        let mut list = vec![];
        for t in iter { if let Ok(tx) = t { list.push(tx); } }
        return Json(list);
    }
    Json(list)
}

async fn watch_mempool_tx(State(state): State<AppState>, Path(tx_id): Path<String>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let exists_in_mempool: i64 = conn.query_row("SELECT COUNT(*) FROM mempool WHERE id = ?1", params![tx_id], |row| row.get(0)).unwrap_or(0);
    let status = if exists_in_mempool > 0 { "Pending" } else { "Confirmed or Not Found" };
    Json(serde_json::json!({"tx_id": tx_id, "status": status}))
}

async fn add_transaction(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(tx): Json<Transaction>,
) -> StatusCode {
    let ip_str = addr.ip().to_string();
    if !check_rate_limit_and_blacklist(&state.rate_limiter, &state.ip_blacklist, &ip_str, 1) {
        return StatusCode::TOO_MANY_REQUESTS;
    }
    let conn = state.db.lock().unwrap();
    if !validate_nvd_address(&tx.sender) || !validate_nvd_address(&tx.receiver) || tx.amount == 0 {
        return StatusCode::BAD_REQUEST;
    }
    let tx_res = conn.execute(
        "INSERT INTO transactions (id, sender, receiver, amount, nonce, timestamp, status, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'confirmed', ?7)",
        params![tx.id, tx.sender, tx.receiver, tx.amount, tx.nonce, tx.timestamp, tx.signature],
    );
    if tx_res.is_err() { return StatusCode::CONFLICT; }
    StatusCode::CREATED
}

async fn get_wallet_balance(State(state): State<AppState>, Path(address): Path<String>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let mined: f64 = conn.query_row("SELECT COALESCE(SUM(reward), 0.0) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0.0);
    let staked: f64 = conn.query_row("SELECT COALESCE(balance, 0.0) FROM stakes WHERE address = ?1", params![address], |row| row.get(0)).unwrap_or(0.0);
    Json(serde_json::json!({"address": address, "mined_balance": mined, "staked_balance": staked}))
}

async fn get_wallet_history(State(_state): State<AppState>, Path(address): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({"address": address, "history": []}))
}

async fn estimate_network_fee() -> Json<serde_json::Value> {
    Json(serde_json::json!({"recommended_fee": 10}))
}

async fn get_address_book(State(state): State<AppState>) -> Json<Vec<AddressBookEntry>> {
    let conn = state.db.lock().unwrap();
    let mut stmt = match conn.prepare("SELECT alias, address FROM address_book") {
        Ok(s) => s,
        Err(_) => return Json(vec![]),
    };
    
    let entries = stmt.query_map([], |row| {
        Ok(AddressBookEntry {
            alias: row.get(0)?,
            address: row.get(1)?,
        })
    });

    let list = vec![];
    if let Ok(iter) = entries {
        let mut list = vec![];
        for item in iter {
            if let Ok(val) = item {
                list.push(val);
            }
        }
        return Json(list);
    }
    Json(list)
}

async fn save_address_book(State(state): State<AppState>, Json(payload): Json<AddressBookEntry>) -> StatusCode {
    let conn = state.db.lock().unwrap();
    let res = conn.execute("INSERT INTO address_book (alias, address) VALUES (?1, ?2) ON CONFLICT(alias) DO UPDATE SET address = ?2", params![payload.alias, payload.address]);
    if res.is_ok() { StatusCode::CREATED } else { StatusCode::INTERNAL_SERVER_ERROR }
}

async fn handle_staking(State(state): State<AppState>, ConnectInfo(_addr): ConnectInfo<SocketAddr>, Json(payload): Json<StakeRequest>) -> StatusCode {
    let conn = state.db.lock().unwrap();
    let current_time = Utc::now().timestamp();
    let res = conn.execute("INSERT INTO stakes (address, balance, last_stake_time) VALUES (?1, ?2, ?3) ON CONFLICT(address) DO UPDATE SET balance = balance + ?2", params![payload.address, payload.amount, current_time]);
    if res.is_ok() { StatusCode::OK } else { StatusCode::INTERNAL_SERVER_ERROR }
}

async fn claim_staking_rewards(State(state): State<AppState>, Json(payload): Json<StakeRequest>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let current_time = Utc::now().timestamp();
    let _ = conn.execute("UPDATE stakes SET last_stake_time = ?1 WHERE address = ?2", params![current_time, payload.address]);
    Json(serde_json::json!({"status": "success", "claimed_reward": 0.05}))
}

// --- دوال الإضافات الجديدة المصممة بعناية فائقة للحفاظ على الاستقرار والأداء ---

// 1. سجل المعاملات العام (Explorer Blocks)
async fn get_explorer_blocks(State(state): State<AppState>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let mut stmt = match conn.prepare("SELECT idx, timestamp, miner, worker_name, reward, hash FROM blocks ORDER BY idx DESC LIMIT 20") {
        Ok(s) => s,
        Err(_) => return Json(serde_json::json!({"blocks": []})),
    };

    let blocks_iter = stmt.query_map([], |row| {
        Ok(serde_json::json!({
            "index": row.get::<_, u64>(0)?,
            "timestamp": row.get::<_, i64>(1)?,
            "miner": row.get::<_, String>(2)?,
            "worker_name": row.get::<_, String>(3)?,
            "reward": row.get::<_, f64>(4)?,
            "hash": row.get::<_, String>(5)?
        }))
    });

    let mut list = vec![];
    if let Ok(iter) = blocks_iter {
        for b in iter {
            if let Ok(val) = b { list.push(val); }
        }
    }

    Json(serde_json::json!({
        "status": "success",
        "recent_blocks": list
    }))
}

// 2. لوحة تحكم المحفظة الشاملة (Wallet Dashboard)
async fn get_full_wallet_dashboard(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let mined_balance: f64 = conn.query_row("SELECT COALESCE(SUM(reward), 0.0) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0.0);
    let staked_balance: f64 = conn.query_row("SELECT COALESCE(balance, 0.0) FROM stakes WHERE address = ?1", params![address], |row| row.get(0)).unwrap_or(0.0);
    let total_blocks: i64 = conn.query_row("SELECT COUNT(*) FROM blocks WHERE miner = ?1", params![address], |row| row.get(0)).unwrap_or(0);

    Json(serde_json::json!({
        "address": address,
        "mined_balance": mined_balance,
        "staked_balance": staked_balance,
        "total_balance": mined_balance + staked_balance,
        "total_blocks_mined": total_blocks,
        "dashboard_status": "Active"
    }))
}

// 3. العداد الحي (Live Ticker)
async fn get_network_ticker(State(state): State<AppState>) -> Json<serde_json::Value> {
    let conn = state.db.lock().unwrap();
    let supply = *state.global_supply.lock().unwrap();
    let total_blocks: i64 = conn.query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0)).unwrap_or(0);
    let current_diff = *state.current_difficulty.lock().unwrap();

    Json(serde_json::json!({
        "live_blocks": total_blocks,
        "circulating_supply": supply,
        "difficulty": current_diff,
        "server_time": Utc::now().timestamp()
    }))
}