# 实时行情系统

使用 C++ 编写 UDP 接收模块（基于 Boost.Asio），解析行情数据包，通过 ZeroMQ 发送给 Rust 中间件。Rust 实现行情聚合计算，通过 TCP 传给 Java。Java 使用 Spring Boot + WebSocket 将行情推送给浏览器客户端。支持客户端订阅特定股票代码。

## 系统架构

```
行情源 → [UDP:5000] → C++ → [ZeroMQ:5555] → Rust → [TCP:6666] → Java → [WebSocket:8080] → 浏览器
```

## 目录结构

```
Java-C-Rust-/
├── PROTOCOL.md          # 通信协议设计文档
├── README.md
├── cpp/                 # C++ 端代码
│   ├── CMakeLists.txt
│   └── main.cpp
├── rust/                # Rust 端代码
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── java/                # Java 端代码
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/marketdata/
│       │   ├── MarketDataApplication.java
│       │   ├── model/
│       │   │   └── AggregatedData.java
│       │   ├── tcp/
│       │   │   └── TcpClientService.java
│       │   └── websocket/
│       │       ├── MarketDataHandler.java
│       │       └── WebSocketConfig.java
│       └── resources/
│           └── application.properties
└── frontend/            # 前端代码
    ├── index.html
    └── app.js
```

## 运行步骤

### 前置依赖

- CMake 3.14+
- Boost 1.70+
- cppzmq
- Rust (with Cargo)
- Java 17+
- Maven

### 1. 启动 C++ 端

```bash
cd cpp
mkdir build && cd build
cmake ..
cmake --build .
./market_data_receiver
```

C++ 端会同时：
- 监听 UDP 5000 端口接收行情数据
- 通过 ZeroMQ PUB 5555 端口发布数据
- 内置模拟行情数据源，会每 500ms 发送一次测试数据

### 2. 启动 Rust 端

```bash
cd rust
cargo run
```

Rust 端会：
- 订阅 ZeroMQ 5555 端口的数据
- 聚合计算（最新价、开盘价、最高价、最低价、成交量）
- 启动 TCP 6666 服务器，等待 Java 连接

### 3. 启动 Java 端

```bash
cd java
mvn spring-boot:run
```

Java 端会：
- 连接 Rust TCP 6666 端口接收数据
- 启动 WebSocket 8080 端口，路径为 `/ws/market`
- 提供静态资源访问

### 4. 打开前端

直接在浏览器中打开 `frontend/index.html`，或把它放在 HTTP 服务器下访问。

点击股票代码按钮进行订阅，查看实时行情数据。

## 通信协议

详细协议设计请查看 [PROTOCOL.md](PROTOCOL.md)
