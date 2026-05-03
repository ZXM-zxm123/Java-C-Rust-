let ws;
const selectedSymbols = new Set();
const marketData = {};
const lastPrices = {};

function initWebSocket() {
    ws = new WebSocket('ws://localhost:8080/ws/market');
    
    ws.onopen = () => {
        document.getElementById('status').textContent = '状态: 已连接';
        document.getElementById('status').style.background = '#c8e6c9';
        if (selectedSymbols.size > 0) {
            sendSubscription();
        }
    };
    
    ws.onclose = () => {
        document.getElementById('status').textContent = '状态: 已断开，正在重连...';
        document.getElementById('status').style.background = '#ffcdd2';
        setTimeout(initWebSocket, 2000);
    };
    
    ws.onerror = (err) => {
        console.error('WebSocket error:', err);
    };
    
    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === 'market_data') {
            updateMarketData(msg.data);
        }
    };
}

function sendSubscription() {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            action: 'subscribe',
            symbols: Array.from(selectedSymbols)
        }));
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
