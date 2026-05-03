let ws;
const selectedSymbols = new Set();
const marketData = {};
const lastPrices = {};
let reconnectAttempts = 0;
let heartbeatTimer = null;
let reconnectTimer = null;
let missedHeartbeats = 0;
let currentFps = 10;
const MAX_MISSED_HEARTBEATS = 3;
const HEARTBEAT_INTERVAL = 3000;
const RECONNECT_INTERVAL = 2000;
const MAX_RECONNECT_ATTEMPTS = 10;

const pendingDataQueue = [];
let isRendering = false;
let lastRenderTime = 0;
let animationFrameId = null;

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
        stopRenderLoop();
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
            if (msg.defaultFps) {
                currentFps = msg.defaultFps;
            }
            break;
            
        case 'batch_update':
            missedHeartbeats = 0;
            queueDataUpdate(msg.data);
            break;
            
        case 'market_data':
            missedHeartbeats = 0;
            queueDataUpdate([msg.data]);
            break;
            
        case 'ping':
            missedHeartbeats = 0;
            sendPong();
            break;
            
        case 'ack':
            if (msg.action === 'fps_set') {
                currentFps = msg.data;
                console.log('FPS updated to:', currentFps);
            }
            break;
            
        default:
            console.log('Unknown message type:', msg.type);
    }
}

function queueDataUpdate(dataArray) {
    for (const data of dataArray) {
        const symbol = data.symbol;
        lastPrices[symbol] = marketData[symbol]?.lastPrice || data.lastPrice;
        marketData[symbol] = data;
        pendingDataQueue.push(data);
    }
    
    if (!isRendering) {
        startRenderLoop();
    }
}

function startRenderLoop() {
    if (animationFrameId !== null) {
        return;
    }
    
    isRendering = true;
    lastRenderTime = performance.now();
    renderLoop();
}

function renderLoop() {
    const now = performance.now();
    const frameInterval = 1000 / currentFps;
    const elapsed = now - lastRenderTime;
    
    if (elapsed >= frameInterval && pendingDataQueue.length > 0) {
        renderBatchUpdate();
        lastRenderTime = now;
    }
    
    if (pendingDataQueue.length > 0 || isRendering) {
        animationFrameId = requestAnimationFrame(renderLoop);
    } else {
        stopRenderLoop();
    }
}

function stopRenderLoop() {
    isRendering = false;
    if (animationFrameId !== null) {
        cancelAnimationFrame(animationFrameId);
        animationFrameId = null;
    }
}

function renderBatchUpdate() {
    if (pendingDataQueue.length === 0) {
        return;
    }
    
    const toProcess = pendingDataQueue.splice(0, pendingDataQueue.length);
    const updatedSymbols = new Set();
    
    for (const data of toProcess) {
        updatedSymbols.add(data.symbol);
    }
    
    const rows = document.querySelectorAll('#marketData tr');
    const existingRows = {};
    rows.forEach(row => {
        existingRows[row.dataset.symbol] = row;
    });
    
    const fragment = document.createDocumentFragment();
    
    for (const symbol of updatedSymbols) {
        const data = marketData[symbol];
        if (!data) continue;
        
        let row = existingRows[symbol];
        if (!row) {
            row = document.createElement('tr');
            row.dataset.symbol = symbol;
            fragment.appendChild(row);
        }
        
        const priceClass = lastPrices[symbol] > data.lastPrice ? 'price-down' : 
                          lastPrices[symbol] < data.lastPrice ? 'price-up' : '';
        
        row.innerHTML = `
            <td>${data.symbol}</td>
            <td class="${priceClass}">${data.lastPrice.toFixed(2)}</td>
            <td>${data.openPrice.toFixed(2)}</td>
            <td>${data.highPrice.toFixed(2)}</td>
            <td>${data.lowPrice.toFixed(2)}</td>
            <td>${data.totalVolume.toLocaleString()}</td>
        `;
        
        if (!existingRows[symbol]) {
            delete existingRows[symbol];
        }
    }
    
    const tbody = document.getElementById('marketData');
    const remainingRows = Array.from(tbody.querySelectorAll('tr'));
    
    for (const row of remainingRows) {
        if (!updatedSymbols.has(row.dataset.symbol)) {
            tbody.removeChild(row);
        }
    }
    
    for (const symbol in existingRows) {
        if (existingRows[symbol].parentNode) {
            tbody.removeChild(existingRows[symbol]);
        }
    }
    
    tbody.appendChild(fragment);
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
            fps: currentFps,
            timestamp: Date.now()
        };
        ws.send(JSON.stringify(message));
        console.log('Subscription sent:', selectedSymbols, 'fps:', currentFps);
    }
}

function setFps(fps) {
    fps = Math.max(1, Math.min(fps, 60));
    currentFps = fps;
    
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: 'setFps', fps: fps }));
    }
    
    const fpsDisplay = document.getElementById('fpsDisplay');
    if (fpsDisplay) {
        fpsDisplay.textContent = `${fps} FPS`;
    }
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

document.addEventListener('DOMContentLoaded', () => {
    const controls = document.querySelector('.controls');
    if (controls) {
        const fpsControl = document.createElement('div');
        fpsControl.style.marginTop = '15px';
        fpsControl.innerHTML = `
            <label style="display: block; margin-bottom: 10px; font-weight: bold;">推送频率:</label>
            <div style="display: flex; gap: 10px; align-items: center;">
                <input type="range" id="fpsSlider" min="1" max="30" value="10" 
                       style="flex: 1;" onchange="setFps(this.value)">
                <span id="fpsDisplay" style="min-width: 60px;">10 FPS</span>
            </div>
        `;
        controls.appendChild(fpsControl);
    }
});

initWebSocket();
