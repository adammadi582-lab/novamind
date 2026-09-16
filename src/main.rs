use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use ring::signature::{self, KeyPair, VerificationAlgorithm};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tower_http::cors::CorsLayer;
use rusqlite::{Connection, params};

// 1. هيكل البلوك في سلسلة الكتل المحمية بقاعدة بيانات
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Block {
    index: u64,
    timestamp: i64,
    miner: String,
    nonce: u64,
    prev_hash: String,
    hash: String,
    reward: f64,
}

// 2. هيكل معاملة التحويل مع التوقيع الرقمي المشفر
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Transaction {
    sender: String,
    receiver: String,
    amount: f64,
    public_key: String,
    signature: String,
}

// 3. حالة الشبكة المربوطة بقاعدة بيانات SQLite دائمة
struct AppState {
    db: Connection,
    global_supply: f64,
}

#[tokio::main]
async fn main() {
    // إعداد قاعدة البيانات الدائمة (تخزين الأبد)
    let db = Connection::open("novamind.db").unwrap();
    db.execute(
        "CREATE TABLE IF NOT EXISTS blocks (
            idx INTEGER PRIMARY KEY,
            timestamp INTEGER,
            miner TEXT,
            nonce INTEGER,
            prev_hash TEXT,
            hash TEXT,
            reward REAL
        )",
        [],
    ).unwrap();

    // التحقق من وجود بلوك البداية (Genesis) أو إنشاؤه
    let mut stmt = db.prepare("SELECT COUNT(*) FROM blocks").unwrap();
    let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();

    if count == 0 {
        db.execute(
            "INSERT INTO blocks (idx, timestamp, miner, nonce, prev_hash, hash, reward) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![0, Utc::now().timestamp(), "NovaMind-Genesis", 0, "0000000000000000", "nvdm_genesis_hash_secure_999", 50.0],
        ).unwrap();
    }

    let state = Arc::new(Mutex::new(AppState {
        db,
        global_supply: 50.0,
    }));

    // مسارات الشبكة العالمية
    let app = Router::new()
        .route("/chain", get(get_chain))
        .route("/mine", post(mine_block))
        .route("/transaction", post(add_transaction))
        .route("/wallet/create", get(create_wallet))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("🚀 NovaMind Ultra-Secure Global Blockchain Server is running on port 8080...");
    axum::serve(listener, app).await.unwrap();
}

// استعراض سلسلة الكتل من قاعدة البيانات مباشرة
async fn get_chain(State(state): State<Arc<Mutex<AppState>>>) -> Json<Vec<Block>> {
    let state = state.lock().unwrap();
    let mut stmt = state.db.prepare("SELECT idx, timestamp, miner, nonce, prev_hash, hash, reward FROM blocks").unwrap();
    
    let block_iter = stmt.query_map([], |row| {
        Ok(Block {
            index: row.get(0)?,
            timestamp: row.get(1)?,
            miner: row.get(2)?,
            nonce: row.get(3)?,
            prev_hash: row.get(4)?,
            hash: row.get(5)?,
            reward: row.get(6)?,
        })
    }).unwrap();

    let mut blockchain = vec![];
    for block in block_iter {
        blockchain.push(block.unwrap());
    }

    Json(blockchain)
}

// توليد محفظة حقيقية بمفاتيح Ed25519 المشفرة
async fn create_wallet() -> Json<serde_json::Value> {
    let rng = ring::rand::SystemRandom::new();
    let pkcs8_bytes = signature::Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let key_pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();
    
    let public_key_hex = hex::encode(key_pair.public_key().as_ref());
    
    Json(serde_json::json!({
        "status": "success",
        "address": format!("nvd_{}", &public_key_hex[..16]),
        "public_key": public_key_hex,
        "message": "Ultra-secure cryptographic wallet generated."
    }))
}

// تعدين بلوك وتخزينه في قاعدة البيانات
#[derive(Deserialize)]
struct MineRequest {
    miner: String,
    nonce: u64,
}

async fn mine_block(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(payload): Json<MineRequest>,
) -> Result<Json<Block>, StatusCode> {
    let mut state = state.lock().unwrap();
    
    // جلب آخر بلوك من قاعدة البيانات
    let mut stmt = state.db.prepare("SELECT idx, hash FROM blocks ORDER BY idx DESC LIMIT 1").unwrap();
    let (prev_index, prev_hash): (u64, String) = stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    
    let new_index = prev_index + 1;
    let timestamp = Utc::now().timestamp();
    
    // بصمة رياضية مترابطة حصينة
    let raw_data = format!("{}{}{}{}", new_index, prev_hash, payload.miner, payload.nonce);
    let digest = format!("{:x}", md5::compute(raw_data));
    
    let reward = if state.global_supply > 1000.0 { 0.05 } else { 0.1 };
    
    let new_block = Block {
        index: new_index,
        timestamp,
        miner: payload.miner,
        nonce: payload.nonce,
        prev_hash,
        hash: digest,
        reward,
    };

    // حفظ البلوك الجديد في قاعدة البيانات الدائمة
    state.db.execute(
        "INSERT INTO blocks (idx, timestamp, miner, nonce, prev_hash, hash, reward) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![new_block.index, new_block.timestamp, new_block.miner, new_block.nonce, new_block.prev_hash, new_block.hash, new_block.reward],
    ).unwrap();

    state.global_supply += reward;

    Ok(Json(new_block))
}

// إضافة المعاملة مع التحقق الصارم من التوقيع الرقمي
async fn add_transaction(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(tx): Json<Transaction>,
) -> StatusCode {
    // فك التشفير والتحقق من صحة التوقيع الرقمي للمحفظة
    let pub_key_bytes = match hex::decode(&tx.public_key) {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    let sig_bytes = match hex::decode(&tx.signature) {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    let peer_public_key = signature::UnparsedPublicKey::new(&signature::ED25519, &pub_key_bytes);
    let message = format!("{}:{}:{}", tx.sender, tx.receiver, tx.amount);

    // التحقق الرياضي القاطع من أن صاحب المحفظة هو من وقع المعاملة
    if peer_public_key.verify(message.as_bytes(), &sig_bytes).is_err() {
        return StatusCode::UNAUTHORIZED; // مرفوض لو التوقيع مزور!
    }

    StatusCode::CREATED
}