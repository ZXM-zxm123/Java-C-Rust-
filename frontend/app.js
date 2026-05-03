let ws;
const selectedSymbols = new Set();
const marketData = {};
const lastPrices = {};
let reconnectAttempts = 0;
let heartbeatTimer = null;
let reconnectTimer = null;
let missedHeartbeats = 0;
const MAX_MISSED_HEARTBEATS = 3;
const HEARTBEAT_INTERVAL = 3000;
const RECONNECT_INTERVAL = 2000;
const MAX_RECONNECT_ATTEMPTS = 10;

function initWebSocket() {
    ws = new WebSocket('ws://localhost:8080/ws/market');
    
    ws.onopen = () => {
        console.log('WebSocket connected');
        reconnectAttempts = 0;
        missedHeartbeats = 0;
        updateConnectionStatus('connected');
        startHeartbeat();
        
        if (selectedSymbols.size > 0) {
            sendSubscription();
        }
    };
    
    ws.onclose = (event) => {
        console.log('WebSocket closed:', event.code, event.reason);
        stopHeartbeat();
        updateConnectionStatus('disconnected');
        
        if (event.code !== 1000) {
            scheduleReconnect();
        }
    };
    
    ws.onerror = (err) => {
        console.error('WebSocket error:', err);
        updateConnectionStatus('error');
    };
    
    ws.onmessage = (event) => {
        handleMessage(event.data);
    };
}

function handleMessage(data) {
    const msg = JSON.parse(data);
    
    switch(msg.type) {
        case 'connected':
            console.log('Server acknowledged connection, session:', msg.sessionId);
            break;
            
        case 'market_data':
            missedHeartbeats = 0;
            updateMarketData(msg.data);
            break;
            
        case 'ping':
            missedHeartbeats = 0;
            sendPong();
            break;
            
        default:
            console.log('Unknown message type:', msg.type);
    }
}

function startHeartbeat() {
    stopHeartbeat();
    
    heartbeatTimer = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            try {
                ws.send(JSON.stringify({ action: 'ping', timestamp: Date.now() }));
                missedHeartbeats++;
                
                if (missedHeartbeats >= MAX_MISSED_HEARTBEATS) {
                    console.warn('Too many missed heartbeats, reconnecting...');
                    ws.close();
                    scheduleReconnect();
                }
            } catch (e) {
                console.error('Error sending ping:', e);
            }
        }
    }, HEARTBEAT_INTERVAL);
}

function stopHeartbeat() {
    if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
    }
}

function sendPong() {
    if (ws && ws.readyState === WebSocket.OPEN) {
        try {
            ws.send(JSON.stringify({ action: 'pong', timestamp: Date.now() }));
        } catch (e) {
            console.error('Error sending pong:', e);
        }
    }
}

function scheduleReconnect() {
    if (reconnectTimer) {
        return;
    }
    
    if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        console.error('Max reconnect attempts reached');
        updateConnectionStatus('failed');
        return;
    }
    
    reconnectAttempts++;
    const delay = RECONNECT_INTERVAL * Math.min(reconnectAttempts, 5);
    
    console.log(`Scheduling reconnect attempt ${reconnectAttempts} in ${delay}ms`);
    updateConnectionStatus('reconnecting');
    
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        initWebSocket();
    }, delay);
}

function updateConnectionStatus(status) {
    const statusEl = document.getElementById('status');
    const statusMap = {
        'connected': { text: '状态: 已连接', bg: '#c8e6c9' },
        'disconnected': { text: '状态: 已断开，正在重连...', bg: '#ffcdd2' },
        'reconnecting': { text: `状态: 正在重连 (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`, bg: '#fff3e0' },
        'error': { text: '状态: 连接错误', bg: '#ffcdd2' },
        'failed': { text: '状态: 连接失败', bg: '#ffcdd2' }
    };
    
    const info = statusMap[status] || statusMap['disconnected'];
    statusEl.textContent = info.text;
    statusEl.style.background = info.bg;
}

function sendSubscription() {
    if (ws && ws.readyState === WebSocket.OPEN) {
        const message = {
            action: 'subscribe',
            symbols: Array.from(selectedSymbols),
            timestamp: Date.now()
        };
        ws.send(JSON.stringify(message));
        console.log('Subscription sent:', selectedSymbols);
    }
}

function updateMarketData(data) {
    const symbol = data.symbol;
    const isNew = !marketData[symbol];
    const prevPrice = lastPrices[symbol];
    
    lastPrices[symbol] = data.lastPrice;
    marketData[symbol] = data;
    
    renderTable();
    
    if (!isNew) {
        const row = document.querySelector(`tr[data-symbol="${symbol}"]`);
        if (row) {
            row.classList.add('updated');
            setTimeout(() => row.classList.remove('updated'), 500);
        }
    }
}

function renderTable() {
    const tbody = document.getElementById('marketData');
    tbody.innerHTML = '';
    
    Object.values(marketData).forEach(data => {
        const row = document.createElement('tr');
        row.dataset.symbol = data.symbol;
        
        const priceClass = lastPrices[data.symbol] > data.lastPrice ? 'price-down' : 
                          lastPrices[data.symbol] < data.lastPrice ? 'price-up' : '';
        
        row.innerHTML = `
            <td>${data.symbol}</td>
            <td class="${priceClass}">${data.lastPrice.toFixed(2)}</td>
            <td>${data.openPrice.toFixed(2)}</td>
            <td>${data.highPrice.toFixed(2)}</td>
            <td>${data.lowPrice.toFixed(2)}</td>
            <td>${data.totalVolume.toLocaleString()}</td>
        `;
        tbody.appendChild(row);
    });
}

document.querySelectorAll('.symbol-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const symbol = btn.dataset.symbol;
        if (selectedSymbols.has(symbol)) {
            selectedSymbols.delete(symbol);
            btn.classList.remove('selected');
        } else {
            selectedSymbols.add(symbol);
            btn.classList.add('selected');
        }
        sendSubscription();
    });
});

initWebSocket();
