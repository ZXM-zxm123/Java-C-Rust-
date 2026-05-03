# Java-C-Rust-
使用 C++ 编写 UDP 接收模块（基于 Boost.Asio），解析行情数据包，通过共享内存（或 ZeroMQ）发送给 Rust 中间件。Rust 实现行情过滤（按股票代码）、聚合计算（如最新价、成交量），通过 TCP 传给 Java。Java 使用 Spring Boot + WebSocket 将行情推送给浏览器客户端。支持客户端订阅特定股票代码。输出三端代码及通信协议设计。
