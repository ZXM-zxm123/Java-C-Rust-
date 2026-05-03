# 实时行情系统通信协议设计

## 1. 总体架构
```
行情源 → [UDP] → C++ → [ZeroMQ PUB/SUB] → Rust → [TCP] → Java → [WebSocket] → 浏览器
```

## 2. C++ 端 (UDP 接收 → ZeroMQ 发布)

### 2.1 UDP 数据包格式（模拟行情源）
```
| 字段         | 长度 (字节) | 类型     | 说明                  |
|-------------|------------|----------|-----------------------|
| magic       | 4          | uint32_t | 魔数 0x53544F43      |
| version     | 1          | uint8_t  | 版本号 0x01            |
| symbol      | 8          | char[8]  | 股票代码，如 "000001"  |
| price       | 8          | double   | 最新价格                |
| volume      | 8          | uint64_t | 成交量                  |
| timestamp   | 8          | int64_t  | 时间戳 (毫秒)           |
```

### 2.2 ZeroMQ 消息格式 (JSON)
```json
{
  "symbol": "000001",
  "price": 10.50,
  "volume": 1000000,
  "timestamp": 1714700000000
}
```

ZeroMQ 配置:
- Socket 类型: PUB
- 地址: tcp://127.0.0.1:5555
- Topic: "market_data"

## 3. Rust 端 (ZeroMQ 订阅 → 过滤聚合 → TCP 发送)

### 3.1 订阅请求格式
Rust 从配置或动态订阅股票代码列表。

### 3.2 聚合数据结构
```json
{
  "symbol": "000001",
  "last_price": 10.50,
  "open_price": 10.00,
  "high_price": 11.00,
  "low_price": 9.80,
  "total_volume": 5000000,
  "update_time": 1714700000000
}
```

### 3.3 TCP 协议格式
```
| 字段         | 长度 (字节) | 类型     | 说明                  |
|-------------|------------|----------|-----------------------|
| length      | 4          | uint32_t | 消息体长度 (大端序)     |
| data        | N          | bytes    | JSON 数据              |
```

TCP 服务器地址: 127.0.0.1:6666

## 4. Java 端 (TCP 接收 → WebSocket 推送)

### 4.1 WebSocket 消息格式

#### 客户端 → 服务器 (订阅请求)
```json
{
  "action": "subscribe",
  "symbols": ["000001", "600000"]
}
```

#### 服务器 → 客户端 (行情推送)
```json
{
  "type": "market_data",
  "data": {
    "symbol": "000001",
    "last_price": 10.50,
    "total_volume": 5000000,
    "timestamp": 1714700000000
  }
}
```

WebSocket 端点: ws://localhost:8080/ws/market
