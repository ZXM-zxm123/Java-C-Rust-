package com.marketdata.tcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marketdata.model.AggregatedData;
import com.marketdata.websocket.MarketDataHandler;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

@Service
public class TcpClientService {
    @Value("${rust.tcp.host}")
    private String host;
    @Value("${rust.tcp.port}")
    private int port;

    private final MarketDataHandler handler;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TcpClientService(MarketDataHandler handler) {
        this.handler = handler;
    }

    @PostConstruct
    public void start() {
        new Thread(this::connectToRust).start();
    }

    private void connectToRust() {
        while (true) {
            try (Socket socket = new Socket(host, port);
                 DataInputStream dis = new DataInputStream(socket.getInputStream())) {
                
                System.out.println("Connected to Rust at " + host + ":" + port);
                
                while (true) {
                    int length = dis.readInt();
                    byte[] buffer = new byte[length];
                    dis.readFully(buffer);
                    
                    AggregatedData data = objectMapper.readValue(buffer, AggregatedData.class);
                    handler.broadcastUpdate(data);
                }
            } catch (IOException e) {
                System.err.println("Connection failed: " + e.getMessage() + ", retrying...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
