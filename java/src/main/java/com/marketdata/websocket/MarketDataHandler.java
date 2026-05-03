package com.marketdata.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marketdata.model.AggregatedData;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Component
public class MarketDataHandler extends TextWebSocketHandler {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<WebSocketSession, Set<String>> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, AggregatedData> latestData = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        subscriptions.put(session, new CopyOnWriteArraySet<>());
        System.out.println("WebSocket connected: " + session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        JsonNode json = objectMapper.readTree(message.getPayload());
        String action = json.path("action").asText();
        
        if ("subscribe".equals(action)) {
            Set<String> symbols = subscriptions.get(session);
            for (JsonNode node : json.path("symbols")) {
                symbols.add(node.asText());
            }
            System.out.println("Subscribed: " + symbols);
            
            symbols.forEach(symbol -> {
                AggregatedData data = latestData.get(symbol);
                if (data != null) {
                    sendUpdate(session, data);
                }
            });
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        subscriptions.remove(session);
        System.out.println("WebSocket disconnected: " + session.getId());
    }

    public void broadcastUpdate(AggregatedData data) {
        latestData.put(data.getSymbol(), data);
        subscriptions.forEach((session, symbols) -> {
            if (symbols.contains(data.getSymbol())) {
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
            e.printStackTrace();
        }
    }
}
