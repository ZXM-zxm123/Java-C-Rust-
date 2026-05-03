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
    private final Map<WebSocketSession, SessionState> sessionStates = new ConcurrentHashMap<>();
    private final Map<String, AggregatedData> latestData = new ConcurrentHashMap<>();
    private final Map<WebSocketSession, Long> lastHeartbeat = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private static final long HEARTBEAT_INTERVAL_MS = 5000;
    private static final long HEARTBEAT_TIMEOUT_MS = 15000;
    private static final long DEFAULT_FRAME_INTERVAL_MS = 100;
    private static final int DEFAULT_FPS = 10;

    private final Map<WebSocketSession, Map<String, AggregatedData>> pendingUpdates = new ConcurrentHashMap<>();
    private volatile long lastFlushTime = 0;

    static class SessionState {
        Set<String> subscriptions = new CopyOnWriteArraySet<>();
        int targetFps = DEFAULT_FPS;
        long lastSentTime = 0;

        SessionState() {}
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessionStates.put(session, new SessionState());
        lastHeartbeat.put(session, System.currentTimeMillis());
        pendingUpdates.put(session, new ConcurrentHashMap<>());
        System.out.println("WebSocket connected: " + session.getId());
        
        startHeartbeatTask();
        startUpdateDispatcher();
        sendWelcomeMessage(session);
    }

    private void startHeartbeatTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkHeartbeats();
                sendHeartbeats();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void startUpdateDispatcher() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                flushPendingUpdates();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, DEFAULT_FRAME_INTERVAL_MS, DEFAULT_FRAME_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private synchronized void flushPendingUpdates() {
        long now = System.currentTimeMillis();
        
        for (Map.Entry<WebSocketSession, SessionState> entry : sessionStates.entrySet()) {
            WebSocketSession session = entry.getKey();
            SessionState state = entry.getValue();
            
            if (!session.isOpen()) {
                continue;
            }

            long frameInterval = 1000 / Math.max(1, state.targetFps);
            if (now - state.lastSentTime < frameInterval) {
                continue;
            }

            Map<String, AggregatedData> sessionPending = pendingUpdates.get(session);
            if (sessionPending == null || sessionPending.isEmpty()) {
                continue;
            }

            Map<String, AggregatedData> toSend = new HashMap<>(sessionPending);
            sessionPending.clear();

            List<AggregatedData> updates = new ArrayList<>();
            for (String symbol : state.subscriptions) {
                AggregatedData data = latestData.get(symbol);
                if (data != null) {
                    AggregatedData pending = toSend.get(symbol);
                    if (pending != null) {
                        updates.add(pending);
                    } else {
                        updates.add(data);
                    }
                }
            }

            if (!updates.isEmpty()) {
                sendBatchUpdate(session, updates);
                state.lastSentTime = now;
            }
        }
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
        
        for (WebSocketSession session : sessionStates.keySet()) {
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
        
        switch (action) {
            case "pong":
                lastHeartbeat.put(session, System.currentTimeMillis());
                break;
            case "ping":
                lastHeartbeat.put(session, System.currentTimeMillis());
                break;
            case "subscribe":
                handleSubscribe(session, json);
                break;
            case "setFps":
                handleSetFps(session, json);
                break;
        }
    }

    private void handleSetFps(WebSocketSession session, JsonNode json) {
        SessionState state = sessionStates.get(session);
        if (state == null) {
            return;
        }

        int fps = json.path("fps").asInt(DEFAULT_FPS);
        fps = Math.max(1, Math.min(fps, 60));
        state.targetFps = fps;
        
        System.out.println("Client " + session.getId() + " set FPS to: " + fps);
        
        sendAckMessage(session, "fps_set", fps);
    }

    @Override
    protected void handlePongMessage(WebSocketSession session, PongMessage message) {
        lastHeartbeat.put(session, System.currentTimeMillis());
    }

    private void handleSubscribe(WebSocketSession session, JsonNode json) {
        SessionState state = sessionStates.computeIfAbsent(session, k -> new SessionState());
        state.subscriptions.clear();
        
        for (JsonNode node : json.path("symbols")) {
            state.subscriptions.add(node.asText());
        }
        
        Integer requestedFps = json.has("fps") ? json.path("fps").asInt() : null;
        if (requestedFps != null) {
            requestedFps = Math.max(1, Math.min(requestedFps, 60));
            state.targetFps = requestedFps;
        }
        
        System.out.println("Client " + session.getId() + " subscribed to: " + state.subscriptions + " @ " + state.targetFps + " fps");
        
        state.subscriptions.forEach(symbol -> {
            AggregatedData data = latestData.get(symbol);
            if (data != null) {
                pendingUpdates.computeIfAbsent(session, k -> new ConcurrentHashMap<>()).put(symbol, data);
            }
        });
    }

    private void sendWelcomeMessage(WebSocketSession session) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "connected");
            message.put("sessionId", session.getId());
            message.put("timestamp", System.currentTimeMillis());
            message.put("defaultFps", DEFAULT_FPS);
            String json = objectMapper.writeValueAsString(message);
            session.sendMessage(new TextMessage(json));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendAckMessage(WebSocketSession session, String action, Object data) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "ack");
            message.put("action", action);
            message.put("data", data);
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
        sessionStates.remove(session);
        lastHeartbeat.remove(session);
        pendingUpdates.remove(session);
    }

    public void broadcastUpdate(AggregatedData data) {
        latestData.put(data.getSymbol(), data);
        
        for (Map.Entry<WebSocketSession, SessionState> entry : sessionStates.entrySet()) {
            WebSocketSession session = entry.getKey();
            SessionState state = entry.getValue();
            
            if (session.isOpen() && state.subscriptions.contains(data.getSymbol())) {
                pendingUpdates.computeIfAbsent(session, k -> new ConcurrentHashMap<>()).put(data.getSymbol(), data);
            }
        }
    }

    private void sendBatchUpdate(WebSocketSession session, List<AggregatedData> updates) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "batch_update");
            message.put("count", updates.size());
            message.put("data", updates);
            message.put("timestamp", System.currentTimeMillis());
            String json = objectMapper.writeValueAsString(message);
            session.sendMessage(new TextMessage(json));
        } catch (IOException e) {
            System.err.println("Error sending batch update to " + session.getId() + ": " + e.getMessage());
            cleanupSession(session);
        }
    }
}
