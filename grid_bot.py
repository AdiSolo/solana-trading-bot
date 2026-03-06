"""
Binance Grid Trading Bot — SOLUSDC
==================================================
- WebSocket pentru preț (zero API weight!)
- WebSocket User Data Stream pentru statusul ordinelor (instantan!)
- ListenKey reînnoit automat la 30 minute
- SELL permis DOAR dacă BUY e confirmat filled
- Pozițiile sunt salvate în Supabase
"""

import time
import os
import threading
import json
from datetime import datetime
import math
import logging
import psycopg2
import psycopg2.extras
import websocket
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ─────────────────────────────────────────────
#  CONFIGURARE
# ─────────────────────────────────────────────

API_KEY      = os.environ.get("BINANCE_API_KEY", "")
API_SECRET   = os.environ.get("BINANCE_API_SECRET", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not API_KEY or not API_SECRET:
    raise ValueError("❌ Lipsesc variabilele BINANCE_API_KEY și BINANCE_API_SECRET!")
if not DATABASE_URL:
    raise ValueError("❌ Lipsește variabila DATABASE_URL!")

SYMBOL         = "SOLUSDC"
SYMBOL_WS      = SYMBOL.lower()
LOWER_PRICE    = 70.0
UPPER_PRICE    = 100.0
GRID_LEVELS    = 10
MIN_PROFIT_PCT = 0.2

# Scaling: cumpărăm mai mult când prețul e mai jos
# Cheia = nivel grid, Valoarea = cantitate SOL
# Nivelele mai înalte (preț mare) = cantitate mică
# Nivelele mai joase (preț mic)   = cantitate mare
def get_order_amount(level_index, grid_levels):
    """Returnează cantitatea pentru un nivel — scaling invers față de preț."""
    # nivel 0 (cel mai jos) → cantitate maximă
    # nivel N (cel mai sus) → cantitate minimă
    min_qty  = 0.1
    max_qty  = 0.5
    step_qty = (max_qty - min_qty) / max(grid_levels - 1, 1)
    qty = max_qty - level_index * step_qty
    return round(max(qty, min_qty), 1)
CHECK_INTERVAL = 5

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  SUPABASE / POSTGRESQL
# ─────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def db_init():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id         SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    level      INTEGER UNIQUE,
                    buy_price  NUMERIC,
                    symbol     TEXT,
                    quantity   NUMERIC,
                    status     TEXT DEFAULT 'pending',
                    order_id   TEXT
                );
            """)
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';")
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS order_id TEXT;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          SERIAL PRIMARY KEY,
                    created_at  TIMESTAMPTZ DEFAULT NOW(),
                    type        TEXT,
                    symbol      TEXT,
                    level       INTEGER,
                    price       NUMERIC,
                    quantity    NUMERIC,
                    profit_usdc NUMERIC DEFAULT 0,
                    profit_pct  NUMERIC DEFAULT 0,
                    order_id    TEXT
                );
            """)
        conn.commit()
    log.info("✅ Supabase conectat și tabelele verificate")

def db_load_positions():
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT level, buy_price, quantity, status, order_id FROM positions WHERE symbol = %s", (SYMBOL,))
            rows = cur.fetchall()
    positions   = {}
    pending     = {}
    quantities  = {}
    for row in rows:
        level = row["level"]
        positions[level]  = float(row["buy_price"])
        quantities[level] = float(row["quantity"] or 0.1)
        if row["status"] == "pending":
            pending[level] = row["order_id"]
    if positions:
        log.info(f"📂 Poziții filled: {[k for k in positions if k not in pending]} | Poziții pending: {list(pending.keys())}")
    else:
        log.info("📂 Nicio poziție salvată anterior — start curat")
    return positions, pending, quantities

def db_save_position(level, buy_price, order_id, quantity, status="pending"):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO positions (level, buy_price, symbol, quantity, status, order_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (level) DO UPDATE
                SET buy_price = EXCLUDED.buy_price, status = EXCLUDED.status,
                    order_id = EXCLUDED.order_id, updated_at = NOW()
            """, (level, buy_price, SYMBOL, quantity, status, order_id))
        conn.commit()
    log.info(f"💾 [DB] Poziție salvată → Nivel {level} @ {buy_price}$ | Qty: {quantity} | Status: {status}")

def db_confirm_position(level):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE positions SET status = 'filled', updated_at = NOW() WHERE level = %s AND symbol = %s", (level, SYMBOL))
        conn.commit()
    log.info(f"✅ [DB] Poziție confirmată filled → Nivel {level}")

def db_delete_position(level):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM positions WHERE level = %s AND symbol = %s", (level, SYMBOL))
        conn.commit()
    log.info(f"🗑️  [DB] Poziție ștearsă → Nivel {level}")

def db_save_trade(trade_type, level, price, quantity, profit_usdc=0, profit_pct=0, order_id=""):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (type, symbol, level, price, quantity, profit_usdc, profit_pct, order_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (trade_type, SYMBOL, level, price, quantity, profit_usdc, profit_pct, order_id))
        conn.commit()
    log.info(f"💾 [DB] Trade salvat → {trade_type} | Nivel {level} | {price}$ | Profit: {profit_usdc:.4f} USDC")

# ─────────────────────────────────────────────
#  BINANCE
# ─────────────────────────────────────────────

def create_client():
    LIVE = os.environ.get("LIVE_TRADING", "false").lower() == "true"
    if LIVE:
        # requests_params evită ping automat la init (reduce weight la pornire)
        client = Client(API_KEY, API_SECRET, {"verify": True, "timeout": 20})
        log.info("✅ Conectat la Binance REAL 💰")
    else:
        client = Client(API_KEY, API_SECRET, testnet=True)
        client.API_URL = "https://testnet.binance.vision/api"
        log.info("✅ Conectat la Binance Testnet")
    return client

def test_trading_permission(client):
    try:
        client.create_test_order(symbol=SYMBOL, side="BUY", type="LIMIT",
                                  timeInForce="GTC", quantity=0.1, price="80.0")
        log.info("✅ Permisiunea de trading verificată!")
    except BinanceAPIException as e:
        log.error(f"❌ EROARE CRITICĂ: {e}")
        raise SystemExit("🛑 Bot oprit — permisiunea de trading lipsește!")

def get_symbol_info(client, symbol):
    info = client.get_symbol_info(symbol)
    price_filter   = next(f for f in info["filters"] if f["filterType"] == "PRICE_FILTER")
    lot_filter     = next(f for f in info["filters"] if f["filterType"] == "LOT_SIZE")
    price_decimals = int(round(-math.log10(float(price_filter["tickSize"]))))
    qty_decimals   = int(round(-math.log10(float(lot_filter["stepSize"]))))
    return price_decimals, qty_decimals

def round_price(price, decimals): return round(price, decimals)
def round_qty(qty, decimals):     return round(qty, decimals)
def calculate_grid_levels(lower, upper, levels):
    step = (upper - lower) / levels
    return [lower + i * step for i in range(levels + 1)]

# ─────────────────────────────────────────────
#  BOT
# ─────────────────────────────────────────────

class GridBot:
    def __init__(self, client):
        self.client      = client
        self.price_dec, self.qty_dec = get_symbol_info(client, SYMBOL)
        self.grid_prices = calculate_grid_levels(LOWER_PRICE, UPPER_PRICE, GRID_LEVELS)

        self.positions, self.pending_orders, self.position_quantities = db_load_positions()
        self.current_price = None
        self.price_lock    = threading.Lock()

        self.profit_total  = 0.0
        self.trades_buy    = 0
        self.trades_sell   = 0
        self.sells_blocked = 0
        self.start_time    = datetime.now()
        self.start_price   = None
        self.prev_level    = None
        self.last_pending_check    = time.time()
        self.last_dashboard_check = time.time()

        step = (UPPER_PRICE - LOWER_PRICE) / GRID_LEVELS
        log.info(f"💎 Simbol: {SYMBOL} | Interval grid: {step:.1f}$ | Nivele: {GRID_LEVELS}")
        log.info(f"📏 Nivele: {[round_price(p, self.price_dec) for p in self.grid_prices]}")

    def get_grid_level(self, price):
        for i in range(len(self.grid_prices) - 1):
            if self.grid_prices[i] <= price < self.grid_prices[i + 1]:
                return i
        return None

    def check_pending_orders(self):
        """Verifică ordinele pending via REST — cu delay între ordine."""
        for level, order_id in list(self.pending_orders.items()):
            time.sleep(60)  # 1 minut între ordine — safe față de rate limit
            try:
                order = self.client.get_order(symbol=SYMBOL, orderId=int(order_id))
                status = order["status"]
                if status == "FILLED":
                    log.info(f"✅ BUY {order_id} @ Nivel {level} — FILLED!")
                    db_confirm_position(level)
                    del self.pending_orders[level]
                elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
                    log.warning(f"❌ BUY {order_id} @ Nivel {level} — {status}, șterg")
                    db_delete_position(level)
                    del self.pending_orders[level]
                    if level in self.positions:
                        del self.positions[level]
            except Exception as e:
                log.error(f"❌ Eroare verificare ordin {order_id}: {e}")

    def place_buy_order(self, price, level_index):
        qty     = round_qty(get_order_amount(level_index, GRID_LEVELS), self.qty_dec)
        price_r = round_price(price, self.price_dec)
        try:
            order    = self.client.create_order(symbol=SYMBOL, side="BUY", type="LIMIT",
                                                 timeInForce="GTC", quantity=qty, price=str(price_r))
            order_id = str(order["orderId"])
            self.positions[level_index]           = price_r
            self.pending_orders[level_index]      = order_id
            self.position_quantities[level_index] = qty
            self.trades_buy += 1
            db_save_position(level_index, price_r, order_id, qty, status="pending")
            db_save_trade("BUY", level_index, price_r, qty, order_id=order_id)
            log.info(f"🟢 BUY LIMIT | Nivel {level_index} | {price_r}$ | ID: {order_id} ⏳")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare BUY: {e}")
            return None

    def place_sell_order(self, sell_price, buy_price, level_index):
        if level_index in self.pending_orders:
            log.warning(f"⏳ SELL BLOCAT | Nivel {level_index} | BUY nu e confirmat încă!")
            self.sells_blocked += 1
            return None
        if sell_price <= buy_price:
            self.sells_blocked += 1
            log.warning(f"🚫 SELL BLOCAT (PIERDERE) | {sell_price}$ ≤ {buy_price}$")
            return None
        profit_pct = (sell_price - buy_price) / buy_price * 100
        if profit_pct < MIN_PROFIT_PCT:
            self.sells_blocked += 1
            log.warning(f"⛔ SELL BLOCAT (PROFIT MIC) | {profit_pct:.2f}% < {MIN_PROFIT_PCT}%")
            return None
        # Citește cantitatea originală din DB
        qty          = round_qty(self.position_quantities.get(level_index, 0.1), self.qty_dec)
        sell_price_r = round_price(sell_price, self.price_dec)
        profit_usdc  = (sell_price_r - buy_price) * qty
        try:
            order    = self.client.create_order(symbol=SYMBOL, side="SELL", type="LIMIT",
                                                 timeInForce="GTC", quantity=qty, price=str(sell_price_r))
            order_id = str(order["orderId"])
            self.profit_total += profit_usdc
            self.trades_sell  += 1
            db_delete_position(level_index)
            db_save_trade("SELL", level_index, sell_price_r, qty, profit_usdc, profit_pct, order_id)
            del self.positions[level_index]
            if level_index in self.position_quantities:
                del self.position_quantities[level_index]
            log.info(f"🔴 SELL LIMIT | Nivel {level_index} | {sell_price_r}$ | Profit: +{profit_usdc:.4f} USDC | ID: {order_id}")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare SELL: {e}")
            return None

    def get_balance(self):
        try:
            account = self.client.get_account()
            usdc = next((float(b["free"]) for b in account["balances"] if b["asset"] == "USDC"), 0.0)
            sol  = next((float(b["free"]) for b in account["balances"] if b["asset"] == "SOL"),  0.0)
            self._last_usdc = usdc
            self._last_sol  = sol
            return usdc, sol
        except Exception:
            return getattr(self, '_last_usdc', 0.0), getattr(self, '_last_sol', 0.0)

    def print_dashboard(self, current_price):
        usdc, sol   = self.get_balance()
        sol_value   = sol * current_price
        total_value = usdc + sol_value
        uptime      = datetime.now() - self.start_time
        hours, rem  = divmod(int(uptime.total_seconds()), 3600)
        minutes     = rem // 60
        price_change = ""
        if self.start_price:
            pct   = (current_price - self.start_price) / self.start_price * 100
            arrow = "📈" if pct >= 0 else "📉"
            price_change = f"{arrow} {pct:+.2f}% față de start"
        filled_str  = " | ".join([f"Nivel {k} @ {v}$" for k, v in sorted(self.positions.items()) if k not in self.pending_orders]) or "niciuna"
        pending_str = " | ".join([f"Nivel {k} @ {v}$" for k, v in sorted(self.positions.items()) if k in self.pending_orders]) or "niciuna"
        print("")
        print("═" * 62)
        print(f"  📊  GRID BOT DASHBOARD  —  {datetime.now().strftime('%H:%M:%S')}")
        print("═" * 62)
        print(f"  💹  SOL preț curent   : {current_price:.2f} USDC  {price_change}")
        print("─" * 62)
        print(f"  💵  Sold USDC liber   : {usdc:.2f} $")
        print(f"  🪙  Sold SOL liber    : {sol:.4f} SOL  (≈ {sol_value:.2f} $)")
        print(f"  💰  Valoare totală    : {total_value:.2f} $")
        print("─" * 62)
        print(f"  ✅  Profit realizat   : +{self.profit_total:.4f} USDC")
        print(f"  🟢  Cumpărări         : {self.trades_buy}")
        print(f"  🔴  Vânzări reușite   : {self.trades_sell}")
        print(f"  🚫  Vânzări blocate   : {self.sells_blocked}")
        print("─" * 62)
        print(f"  ✅  Poziții filled    : {filled_str}")
        print(f"  ⏳  Poziții pending   : {pending_str}")
        print(f"  ⏱️   Uptime sesiune    : {hours}h {minutes}m")
        print("═" * 62)
        print("")

    def process_price(self, current_price):
        current_level = self.get_grid_level(current_price)

        if self.start_price is None:
            self.start_price = current_price

        now = time.time()

        # Verificare pending la fiecare 20 minute
        if now - self.last_pending_check >= 1200 and self.pending_orders:
            self.check_pending_orders()
            self.last_pending_check = now

        if now - self.last_dashboard_check >= 3600:
            self.print_dashboard(current_price)
            self.last_dashboard_check = now

        if current_level is None:
            log.warning(f"⚠️  Preț {current_price} în afara gridului")
            self.prev_level = None
            return

        log.info(f"💰 Preț: {current_price} | Nivel: {current_level} | Filled: {len(self.positions) - len(self.pending_orders)} | Pending: {len(self.pending_orders)}")

        # La primul tick după pornire, doar setăm nivelul de referință
        # fără să plasăm ordine — evităm semnale false la restart
        if self.prev_level is None:
            log.info(f"📍 Nivel inițial setat: {current_level} — fără ordine la primul tick")
            self.prev_level = current_level
            return

        # Salt mare de nivel = gap de preț — ignorăm pentru a evita ordine greșite
        level_diff = abs(current_level - self.prev_level)
        if level_diff > 1:
            log.warning(f"⚠️  Salt nivel: {self.prev_level} → {current_level} (diff={level_diff}) — ignorat")
            self.prev_level = current_level
            return

        if current_level < self.prev_level:
            buy_price = self.grid_prices[current_level]
            if current_level not in self.positions:
                self.place_buy_order(buy_price, current_level)

        elif current_level > self.prev_level:
            sell_price = self.grid_prices[current_level]
            if self.prev_level in self.positions:
                buy_price = self.positions[self.prev_level]
                self.place_sell_order(sell_price, buy_price, self.prev_level)

        self.prev_level = current_level

    def start_price_websocket(self):
        """WebSocket 1: prețul curent — zero API weight."""
        WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL_WS}@ticker"

        def on_message(ws, message):
            try:
                data = json.loads(message)
                with self.price_lock:
                    self.current_price = float(data["c"])
            except Exception as e:
                log.error(f"❌ Price WS eroare: {e}")

        def on_open(ws):
            log.info(f"📡 Price WebSocket conectat — zero API weight!")

        def on_error(ws, error):
            log.error(f"❌ Price WS error: {error}")

        def on_close(ws, *args):
            log.warning("⚠️ Price WS închis — reconectez...")

        def run_ws():
            while True:
                try:
                    ws = websocket.WebSocketApp(WS_URL, on_message=on_message,
                                                 on_open=on_open, on_error=on_error, on_close=on_close)
                    ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception as e:
                    log.error(f"❌ Price WS crash: {e}")
                time.sleep(5)

        threading.Thread(target=run_ws, daemon=True).start()

    def run(self):
        log.info("🚀 Botul a pornit cu WebSocket!")
        self.start_price_websocket()

        log.info("⏳ Aștept primul preț de la WebSocket...")
        while self.current_price is None:
            time.sleep(1)
        log.info(f"✅ Primul preț primit: {self.current_price}$")

        last_processed_price = None

        try:
            while True:
                with self.price_lock:
                    price = self.current_price

                if price is not None and price != last_processed_price:
                    last_processed_price = price
                    try:
                        self.process_price(price)
                    except BinanceAPIException as e:
                        log.error(f"❌ Eroare API: {e}")
                        if "-1003" in str(e):
                            log.warning("⏳ Rate limit detectat — aștept 5 minute...")
                            time.sleep(300)
                    except Exception as e:
                        log.error(f"❌ Eroare neașteptată: {e}")

                time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            log.info("🛑 Bot oprit de utilizator.")

# ─────────────────────────────────────────────
#  PORNIRE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    db_init()
    while True:
        try:
            client = create_client()
            break
        except BinanceAPIException as e:
            if "-1003" in str(e):
                log.warning("⏳ IP banat la pornire — aștept 5 minute înainte de retry...")
                time.sleep(300)
            else:
                raise
    test_trading_permission(client)
    bot = GridBot(client)
    bot.run()