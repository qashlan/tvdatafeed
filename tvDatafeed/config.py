"""Configuration constants for tvDatafeed."""

# TradingView URLs
WS_URL = "wss://data.tradingview.com/socket.io/websocket"
SIGN_IN_URL = "https://www.tradingview.com/accounts/signin/"
SEARCH_URL = "https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production"

# WebSocket settings
WS_TIMEOUT = 5
RECV_TIMEOUT = 30

# Live feed retry settings
RETRY_LIMIT = 50

# Consumer queue settings
CONSUMER_QUEUE_MAXSIZE = 100
CONSUMER_QUEUE_PUT_TIMEOUT = 5
