package com.marketdata.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marketdata.model.AggregatedData;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

@Component
public class MarketDataHandler extends TextWebSocketHandler {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<WebSocketSession, Set<String>> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, AggregatedData> latestData = new ConcurrentHashMap<>();
    private final Map<WebSocketSession, Long> lastHeartbeat = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(2);
    private static final long HEARTBEAT_INTERVAL_MS = 5000;
    private static final long HEARTBEAT_TIMEOUT_MS = 15000;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        subscriptions.put(session, new CopyOnWriteArraySet<>());
        lastHeartbeat.put(session, System.currentTimeMillis());
        System.out.println("WebSocket connected: " + session.getId());
        
        startHeartbeatTask();
        sendWelcomeMessage(session);
    }

    private void startHeartbeatTask() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                checkHeartbeats();
                sendHeartbeats();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        List<WebSocketSession> deadSessions = new ArrayList<>();
        
        for (Map.Entry<WebSocketSession, Long> entry : lastHeartbeat.entrySet()) {
            if (now - entry.getValue() > HEARTBEAT_TIMEOUT_MS) {
                deadSessions.add(entry.getKey());
            }
        }
        
        for (WebSocketSession session : deadSessions) {
            System.out.println("Heartbeat timeout, closing dead session: " + session.getId());
            try {
                session.close(CloseStatus.GOING_AWAY);
            } catch (IOException e) {
                e.printStackTrace();
            }
            cleanupSession(session);
        }
    }

    private void sendHeartbeats() {
        byte[] pingData = "ping".getBytes();
        ByteBuffer payload = ByteBuffer.wrap(pingData);
        
        for (WebSocketSession session : subscriptions.keySet()) {
            if (session.isOpen()) {
                try {
                    session.sendMessage(new PingMessage(payload));
                    lastHeartbeat.put(session, System.currentTimeMillis());
                } catch (IOException e) {
                    System.err.println("Failed to send heartbeat to " + session.getId() + ": " + e.getMessage());
                    cleanupSession(session);
                }
            }
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        JsonNode json = objectMapper.readTree(payload);
        String action = json.path("action").asText();
        
        if ("pong".equals(action)) {
            lastHeartbeat.put(session, System.currentTimeMillis());
            System.out.println("Received pong from client: " + session.getId());
        } else if ("subscribe".equals(action)) {
            handleSubscribe(session, json);
        } else if ("ping".equals(action)) {
            lastHeartbeat.put(session, System.currentTimeMillis());
        }
    }

    @Override
    protected void handlePongMessage(WebSocketSession session, PongMessage message) {
        lastHeartbeat.put(session, System.currentTimeMillis());
        System.out.println("Received pong from " + session.getId());
    }

    private void handleSubscribe(WebSocketSession session, JsonNode json) {
        Set<String> symbols = subscriptions.computeIfAbsent(session, k -> new CopyOnWriteArraySet<>());
        symbols.clear();
        
        for (JsonNode node : json.path("symbols")) {
            symbols.add(node.asText());
        }
        System.out.println("Client " + session.getId() + " subscribed to: " + symbols);
        
        symbols.forEach(symbol -> {
            AggregatedData data = latestData.get(symbol);
            if (data != null) {
                sendUpdate(session, data);
            }
        });
    }

    private void sendWelcomeMessage(WebSocketSession session) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "connected");
            message.put("sessionId", session.getId());
            message.put("timestamp", System.currentTimeMillis());
            String json = objectMapper.writeValueAsString(message);
            session.sendMessage(new TextMessage(json));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        System.out.println("WebSocket disconnected: " + session.getId() + ", status: " + status);
        cleanupSession(session);
    }

    private void cleanupSession(WebSocketSession session) {
        subscriptions.remove(session);
        lastHeartbeat.remove(session);
    }

    public void broadcastUpdate(AggregatedData data) {
        latestData.put(data.getSymbol(), data);
        subscriptions.forEach((session, symbols) -> {
            if (session.isOpen() && symbols.contains(data.getSymbol())) {
                sendUpdate(session, data);
            }
        });
    }

    private void sendUpdate(WebSocketSession session, AggregatedData data) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "market_data");
            message.put("data", data);
            String json = objectMapper.writeValueAsString(message);
            session.sendMessage(new TextMessage(json));
        } catch (IOException e) {
            System.err.println("Error sending update to " + session.getId() + ": " + e.getMessage());
            cleanupSession(session);
        }
    }
}
