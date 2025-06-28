import asyncio
import sys
import os
from loguru import logger
import redis
import time
from dydx_v4_client.indexer.socket.websocket import IndexerSocket
from network import MAINNET
from RedisThrottler import RedisThrottler
from generate_strategy_params import get_active_strategy_tickers, order_book_feed_config
import argparse
from datetime import datetime

# Paths to include (adjust as needed for your environment)
sys.path.append('/home/somer/v4-clients/v4-client-py-v2')
sys.path.append('/home/somer/v4-clients/v4-client-py-v2/tests')

# Argument parser for command-line arguments
parser = argparse.ArgumentParser(description='Run the DYDX V4 Order Book Feed.')
parser.add_argument('config', help='Configuration file for order book feed')
parser.add_argument('--log-level', default='INFO', help='Set the log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
parser.add_argument('--stale-threshold', type=int, default=120, help='Time in seconds before considering a symbol stale')
parser.add_argument('--log-dir', default='./logsv4', help='Directory to store log files')
args = parser.parse_args()

# Configure proper logging with clean filenames
try:
    # Ensure log directory exists with absolute path
    log_dir = os.path.abspath(args.log_dir)
    os.makedirs(log_dir, exist_ok=True)
    
    # Extract config name for the log filename
    config_name = os.path.basename(args.config).replace('.json', '')
    
    # Create clean filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_filename = f"orderbook_feed_{config_name}_{timestamp}"
    log_file = os.path.join(log_dir, f"{base_filename}.log")
    
    # Remove any existing handlers
    logger.remove()
    
    # Add compressed file handler
    logger.add(
        f"{log_file}",  # Explicitly add .gz extension 
        level=args.log_level.upper(),
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
        compression="gz",  
        enqueue=True,       # Use queue for thread safety
        diagnose=False,     # Disable traceback to reduce file size
        backtrace=False,    # Disable backtrace to reduce file size
        catch=True          # Catch exceptions
    )
    
    # Print information to console
    print(f"Starting DYDX V4 Order Book Feed...")
    print(f"Using config: {args.config}")
    print(f"Logs are being written to: {log_file}.gz (compressed)")
    print(f"Log level: {args.log_level.upper()}")
    
except Exception as e:
    print(f"ERROR setting up logging: {e}")
    print("Continuing with console logging only...")
    logger.remove()
    logger.add(sys.stderr, level="INFO")

# Start of application logging
logger.info(f"DYDX V4 Order Book Feed started with config: {args.config}")
logger.info(f"Log directory: {args.log_dir}")
logger.info(f"PYTHONPATH: {sys.path}")

# Initialize Redis and Redis throttler
redis_client = redis.Redis(host='localhost', port=6379, db=0)
throttler = RedisThrottler(redis_client, min_interval=2.0)  # 2 second throttle

# List of symbols to subscribe to
symbols = order_book_feed_config(args.config)
logger.info(f"Subscribing to symbols: {symbols}")

# Initialize order book tracker
order_book_tracker = {symbol: {'bids': [], 'asks': []} for symbol in symbols}
logger.info("Initialized order book tracker.")

# Dictionary to track last update time for each symbol
last_update_times = {symbol: time.time() for symbol in symbols}
# Dictionary to track subscription status
subscription_status = {symbol: True for symbol in symbols}  # Initially all are subscribed

# Function to log and validate order book state
def log_order_book_state(symbol):
    top_bids = order_book_tracker[symbol]['bids']
    top_asks = order_book_tracker[symbol]['asks']
    logger.debug(f"Symbol: {symbol}, Top 3 Bids: {top_bids}, Top 3 Asks: {top_asks}")

    # Only check for crossed books if we have both bids and asks
    if top_bids and top_asks and top_bids[0][0] >= top_asks[0][0]:
        logger.error(f"Invalid order book state for {symbol}: top bid ({top_bids[0][0]}) >= top ask ({top_asks[0][0]})")
        # Reset the order book for this symbol to prevent persisting invalid state
        order_book_tracker[symbol] = {'bids': [], 'asks': []}
        logger.info(f"Reset order book for {symbol} due to crossed book condition")

# Function to manage order book updates and keep only the top 3 bids/asks
def update_order_book(symbol, side, price, size):
    logger.debug(f"Updating order book for {symbol}: {side} {price} {size}")
    order_book = order_book_tracker.setdefault(symbol, {'bids': [], 'asks': []})

    # Update the last update timestamp for this symbol
    last_update_times[symbol] = time.time()

    price = float(price)
    size = float(size)
    opposite_side = 'asks' if side == 'bids' else 'bids'

    # Remove any crossing entries from the opposite side
    if size > 0:
        order_book[opposite_side] = [entry for entry in order_book[opposite_side] if entry[0] != price]
        logger.debug(f"Removed crossing entries from {opposite_side} for {symbol} at price {price}")

    # Remove the price level if size is 0 or update/add the price level
    if size == 0:
        order_book[side] = [entry for entry in order_book[side] if entry[0] != price]
        logger.debug(f"Removed price level for {symbol}: {side} {price}")
    else:
        found = False
        for i, entry in enumerate(order_book[side]):
            if entry[0] == price:
                order_book[side][i] = (price, size)
                logger.debug(f"Updated order book for {symbol}: {side} {price} {size}")
                found = True
                break
        if not found:
            order_book[side].append((price, size))
            logger.debug(f"Added to order book for {symbol}: {side} {price} {size}")

    # Sort and trim the order book after update
    if side == 'bids':
        order_book[side].sort(reverse=True, key=lambda x: x[0])
    else:
        order_book[side].sort(key=lambda x: x[0])
    order_book[side] = order_book[side][:3]  # Keep only top 3

    # Prepare Redis update strings
    top_bids_str = str(order_book['bids'][:3])
    top_asks_str = str(order_book['asks'][:3])

    # Update Redis with throttling
    if side == 'bids':
        throttler.update(f'{symbol}_top_bids', top_bids_str)
        throttler.update(f'{symbol}_bids_update_channel', 'Update')
    else:
        throttler.update(f'{symbol}_top_asks', top_asks_str)
        throttler.update(f'{symbol}_asks_update_channel', 'Update')

    # Log order book state
    log_order_book_state(symbol)

# WebSocket message handler
def handle_message(ws: IndexerSocket, message: dict):
    global last_message_time
    # Update the last message time whenever we receive any message
    last_message_time = asyncio.get_event_loop().time()
    
    if message["type"] == "connected":
        logger.info("WebSocket connected, subscribing to order books.")
        ws.markets.subscribe()  # General market subscription
        logger.info("Subscribed to markets channel.")
        for symbol in symbols:
            ws.order_book.subscribe(symbol)  # Subscribe to order book data for each symbol
            logger.info(f"Subscribed to order book for {symbol}")
            subscription_status[symbol] = True
    elif message["type"] == "channel_data" or message["type"] == "channel_batch_data":
        symbol = message.get('id')
        if symbol:
            # Handle single update (channel_data)
            if message["type"] == "channel_data" and 'contents' in message:
                parse_order_book_contents(symbol, message['contents'])
            # Handle batch update (channel_batch_data)
            elif message["type"] == "channel_batch_data" and 'contents' in message:
                for entry in message['contents']:
                    parse_order_book_contents(symbol, entry)
    elif message["type"] == "error":
        # Handle subscription errors
        symbol = message.get('id')
        if symbol and symbol in symbols:
            logger.error(f"Error for symbol {symbol}: {message.get('message', 'Unknown error')}")
            subscription_status[symbol] = False
            
            # Check if all subscriptions have failed
            active_subscriptions = sum(1 for status in subscription_status.values() if status)
            if active_subscriptions == 0:
                logger.critical("❌ All subscriptions have failed! Will force reconnection.")
                # Use a task to close the connection after a short delay
                asyncio.create_task(force_reconnect(ws))

# Add force reconnect function
async def force_reconnect(ws):
    """Force reconnection when all subscriptions fail."""
    logger.warning("Forcing reconnection due to subscription failures...")
    await asyncio.sleep(1)  # Brief delay
    try:
        await ws.close()
        logger.info("WebSocket closed for reconnection")
    except Exception as e:
        logger.error(f"Error closing WebSocket during forced reconnect: {e}")

# Helper function to parse individual order book contents
def parse_order_book_contents(symbol, contents):
    for side, updates in (('bids', contents.get('bids', [])),
                          ('asks', contents.get('asks', []))):
        for price, size in updates:
            update_order_book(symbol, side, price, size)

# Function to check for stale symbols and resubscribe
async def check_stale_symbols(ws, stale_threshold=120):
    while True:
        await asyncio.sleep(10)  # Check every 10 seconds
        current_time = time.time()
        stale_symbols = []
        
        # Check for stale symbols (no updates for more than threshold seconds)
        for symbol in symbols:
            if current_time - last_update_times.get(symbol, 0) > stale_threshold:
                logger.warning(f"⚠️ Symbol {symbol} has not been updated for {stale_threshold} seconds")
                stale_symbols.append(symbol)
        
        # Check for symbols with subscription errors
        error_symbols = [symbol for symbol, status in subscription_status.items() if not status]
        
        # Combine stale and error symbols
        problem_symbols = list(set(stale_symbols + error_symbols))
        
        # Resubscribe to problem symbols
        for symbol in problem_symbols:
            try:
                logger.info(f"🔄 Resubscribing to symbol {symbol}...")
                # Unsubscribe first if needed
                try:
                    ws.order_book.unsubscribe(symbol)
                    logger.info(f"Successfully unsubscribed from {symbol}")
                except Exception as e:
                    logger.warning(f"Failed to unsubscribe from {symbol}: {e}")
                
                # Small delay between unsubscribe and subscribe
                await asyncio.sleep(1)
                
                # Resubscribe
                ws.order_book.subscribe(symbol)
                logger.info(f"✅ Successfully resubscribed to {symbol}")
                
                # Reset status and update time
                subscription_status[symbol] = True
                last_update_times[symbol] = current_time
                
            except Exception as e:
                logger.error(f"❌ Failed to resubscribe to {symbol}: {e}")

# Connection monitoring and retry logic
# Create a global variable to track the last message time
last_message_time = 0

async def monitor_connection(ws):
    """Monitor for connection issues with improved timeouts."""
    global last_message_time
    
    # Initialize on function start
    last_message_time = asyncio.get_event_loop().time()
    
    while True:
        await asyncio.sleep(15)  # Check every 15 seconds
        current_time = asyncio.get_event_loop().time()
        
        # Check if we've received any messages recently
        if current_time - last_message_time > 30:  # Increased timeout to 30 seconds
            logger.warning("⏱️ No messages received for 30 seconds, forcing reconnection...")
            await force_reconnect(ws)
            return
            
        # Check if we have a severely impaired feed (most symbols not updating)
        current_time_unix = time.time()
        stale_count = sum(1 for symbol, last_time in last_update_times.items() 
                        if current_time_unix - last_time > 60)  # 1 minute threshold
        
        if stale_count >= len(symbols) * 0.75:  # If 75% of symbols are stale
            logger.warning(f"⚠️ {stale_count}/{len(symbols)} symbols not updating for 60+ seconds, forcing reconnection...")
            await force_reconnect(ws)
            return

# Main function to run WebSocket connection with retry logic
async def main():
    while True:
        try:
            # Reset all tracking dictionaries on reconnect
            for symbol in symbols:
                subscription_status[symbol] = True
                last_update_times[symbol] = time.time()
                order_book_tracker[symbol] = {'bids': [], 'asks': []}
                
            ws = IndexerSocket(MAINNET.websocket_indexer, on_message=handle_message)
            
            # Task to monitor connection with improved timeout handling
            monitor_task = asyncio.create_task(monitor_connection(ws))
            
            # Task to check for stale symbols
            stale_check_task = asyncio.create_task(check_stale_symbols(ws, args.stale_threshold))

            # Connect with timeout
            try:
                connect_task = asyncio.create_task(ws.connect())
                await asyncio.wait_for(connect_task, timeout=30)  # 30 second connection timeout
                logger.info("WebSocket connection established.")
            except asyncio.TimeoutError:
                logger.error("Connection timed out after 30 seconds")
                # Cancel monitoring tasks
                monitor_task.cancel()
                stale_check_task.cancel()
                await asyncio.sleep(5)
                continue

            # Wait for either task to complete (only happens when reconnection is needed)
            done, pending = await asyncio.wait(
                [monitor_task, stale_check_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel any pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
            logger.info("Reconnection initiated by monitoring task")
            
        except Exception as e:
            logger.error(f"Connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

# Run the WebSocket connection
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application terminated by user")
        logger.info("Application terminated by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        logger.critical(f"Fatal error: {e}")
