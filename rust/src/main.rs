use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ZMQ_ADDR: &str = "tcp://127.0.0.1:5555";
const TCP_ADDR: &str = "127.0.0.1:6666";

#[derive(Debug, Deserialize, Clone)]
struct RawMarketData {
    symbol: String,
    price: f64,
    volume: u64,
    timestamp: i64,
}

#[derive(Debug, Serialize, Clone)]
struct AggregatedData {
    symbol: String,
    last_price: f64,
    open_price: f64,
    high_price: f64,
    low_price: f64,
    total_volume: u64,
    update_time: i64,
}

struct MarketAggregator {
    data: Arc<Mutex<HashMap<String, AggregatedData>>>,
}

impl MarketAggregator {
    fn new() -> Self {
        MarketAggregator {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn update(&self, raw: RawMarketData) {
        let mut data = self.data.lock().unwrap();
        if let Some(agg) = data.get_mut(&raw.symbol) {
            agg.last_price = raw.price;
            agg.high_price = agg.high_price.max(raw.price);
            agg.low_price = agg.low_price.min(raw.price);
            agg.total_volume += raw.volume;
            agg.update_time = raw.timestamp;
        } else {
            data.insert(raw.symbol.clone(), AggregatedData {
                symbol: raw.symbol,
                last_price: raw.price,
                open_price: raw.price,
                high_price: raw.price,
                low_price: raw.price,
                total_volume: raw.volume,
                update_time: raw.timestamp,
            });
        }
    }

    fn get_all(&self) -> Vec<AggregatedData> {
        let data = self.data.lock().unwrap();
        data.values().cloned().collect()
    }
}

async fn handle_client(mut stream: TcpStream, aggregator: Arc<MarketAggregator>) {
    println!("Java client connected");
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
    
    loop {
        interval.tick().await;
        let data = aggregator.get_all();
        for agg in data {
            if let Err(e) = send_aggregated_data(&mut stream, &agg).await {
                eprintln!("Error sending data: {}", e);
                return;
            }
        }
    }
}

async fn send_aggregated_data(stream: &mut TcpStream, data: &AggregatedData) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string(data)?;
    let bytes = json.as_bytes();
    let len = (bytes.len() as u32).to_be_bytes();
    
    stream.write_all(&len).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    
    Ok(())
}

fn run_zmq_subscriber(aggregator: Arc<MarketAggregator>) {
    let ctx = zmq::Context::new();
    let subscriber = ctx.socket(zmq::SUB).unwrap();
    subscriber.connect(ZMQ_ADDR).unwrap();
    subscriber.set_subscribe(b"market_data").unwrap();
    
    println!("ZeroMQ subscriber connected to {}", ZMQ_ADDR);
    
    loop {
        let topic = subscriber.recv_msg(0).unwrap();
        let msg = subscriber.recv_msg(0).unwrap();
        
        if let Ok(raw) = serde_json::from_slice::<RawMarketData>(&msg) {
            println!("Received: {} @ {}", raw.symbol, raw.price);
            aggregator.update(raw);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let aggregator = Arc::new(MarketAggregator::new());
    println!("Rust Market Data Aggregator starting...");
    
    let aggregator_clone = Arc::clone(&aggregator);
    std::thread::spawn(move || run_zmq_subscriber(aggregator_clone));
    
    let listener = TcpListener::bind(TCP_ADDR).await?;
    println!("TCP server listening on {}", TCP_ADDR);
    
    while let Ok((stream, _)) = listener.accept().await {
        let agg_clone = Arc::clone(&aggregator);
        tokio::spawn(handle_client(stream, agg_clone));
    }
    
    Ok(())
}
