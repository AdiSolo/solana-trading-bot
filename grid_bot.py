"""
Binance Grid Trading Bot — SOLUSDC
==================================================
- Plasează ordine LIMIT la prețurile gridului
- SELL permis DOAR dacă BUY e confirmat "filled" în baza de date
- VINDE DOAR dacă prețul de vânzare > prețul de cumpărare (profit garantat)
- Pozițiile sunt salvate în Supabase — supraviețuiesc repornirilor!
- Toate tranzacțiile sunt salvate în Supabase pentru istoric complet
"""

import time
import os
from datetime import datetime
import math
import logging
import psycopg2
import psycopg2.extras
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
LOWER_PRICE    = 70.0
UPPER_PRICE    = 100.0
GRID_LEVELS    = 10
ORDER_AMOUNT   = 0.1
MIN_PROFIT_PCT = 0.2
CHECK_INTERVAL = 30

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
    """Creează tabelele dacă nu există. Adaugă coloanele noi dacă lipsesc."""
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
    """Încarcă pozițiile din DB — atât pending cât și filled."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT level, buy_price, status, order_id
                FROM positions WHERE symbol = %s
            """, (SYMBOL,))
            rows = cur.fetchall()

    positions = {}
    pending   = {}
    for row in rows:
        level = row["level"]
        positions[level] = float(row["buy_price"])
        if row["status"] == "pending":
            pending[level] = row["order_id"]

    if positions:
        filled_levels  = [k for k in positions if k not in pending]
        pending_levels = list(pending.keys())
        log.info(f"📂 Poziții filled: {filled_levels} | Poziții pending: {pending_levels}")
    else:
        log.info("📂 Nicio poziție salvată anterior — start curat")

    return positions, pending

def db_save_position(level, buy_price, order_id, status="pending"):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO positions (level, buy_price, symbol, quantity, status, order_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (level) DO UPDATE
                SET buy_price  = EXCLUDED.buy_price,
                    status     = EXCLUDED.status,
                    order_id   = EXCLUDED.order_id,
                    updated_at = NOW()
            """, (level, buy_price, SYMBOL, ORDER_AMOUNT, status, order_id))
        conn.commit()
    log.info(f"💾 [DB] Poziție salvată → Nivel {level} @ {buy_price}$ | Status: {status}")

def db_confirm_position(level):
    """Marchează o poziție ca filled în DB."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE positions SET status = 'filled', updated_at = NOW()
                WHERE level = %s AND symbol = %s
            """, (level, SYMBOL))
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
    log.info(f"💾 [DB] Tranzacție salvată → {trade_type} | Nivel {level} | {price}$ | Profit: {profit_usdc:.4f} USDC")

# ─────────────────────────────────────────────
#  BINANCE
# ─────────────────────────────────────────────

def create_client():
    LIVE = os.environ.get("LIVE_TRADING", "false").lower() == "true"
    if LIVE:
        client = Client(API_KEY, API_SECRET)
        log.info("✅ Conectat la Binance REAL 💰")
    else:
        client = Client(API_KEY, API_SECRET, testnet=True)
        client.API_URL = "https://testnet.binance.vision/api"
        log.info("✅ Conectat la Binance Testnet")
    return client

def test_trading_permission(client):
    try:
        client.create_test_order(
            symbol=SYMBOL, side="BUY", type="LIMIT",
            timeInForce="GTC", quantity=0.1, price="80.0"
        )
        log.info("✅ Permisiunea de trading verificată — botul poate plasa ordine!")
    except BinanceAPIException as e:
        log.error(f"❌ EROARE CRITICĂ: {e}")
        raise SystemExit("🛑 Bot oprit — permisiunea de trading lipsește!")

def check_order_status(client, order_id):
    """Verifică statusul unui ordin pe Binance."""
    try:
        order = client.get_order(symbol=SYMBOL, orderId=int(order_id))
        status = order["status"]
        if status == "FILLED":
            return "filled"
        elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
            return "canceled"
        return "pending"
    except Exception as e:
        log.error(f"❌ Eroare verificare ordin {order_id}: {e}")
        return "pending"

def get_current_price(client, symbol):
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker["price"])

def get_symbol_info(client, symbol):
    info = client.get_symbol_info(symbol)
    price_filter   = next(f for f in info["filters"] if f["filterType"] == "PRICE_FILTER")
    lot_filter     = next(f for f in info["filters"] if f["filterType"] == "LOT_SIZE")
    tick_size      = float(price_filter["tickSize"])
    step_size      = float(lot_filter["stepSize"])
    price_decimals = int(round(-math.log10(tick_size)))
    qty_decimals   = int(round(-math.log10(step_size)))
    return price_decimals, qty_decimals

def round_price(price, decimals):
    return round(price, decimals)

def round_qty(qty, decimals):
    return round(qty, decimals)

def calculate_grid_levels(lower, upper, levels):
    step = (upper - lower) / levels
    return [lower + i * step for i in range(levels + 1)]

# ─────────────────────────────────────────────
#  BOT
# ─────────────────────────────────────────────

class GridBot:
    def __init__(self, client):
        self.client = client
        self.price_dec, self.qty_dec = get_symbol_info(client, SYMBOL)
        self.grid_prices = calculate_grid_levels(LOWER_PRICE, UPPER_PRICE, GRID_LEVELS)

        # positions      = {level: buy_price}  — toate pozițiile
        # pending_orders = {level: order_id}   — doar cele neexecutate pe Binance
        self.positions, self.pending_orders = db_load_positions()

        self.profit_total  = 0.0
        self.trades_buy    = 0
        self.trades_sell   = 0
        self.sells_blocked = 0
        self.start_time    = datetime.now()
        self.start_price   = None

        step          = (UPPER_PRICE - LOWER_PRICE) / GRID_LEVELS
        total_capital = ORDER_AMOUNT * LOWER_PRICE * GRID_LEVELS
        log.info(f"💎 Simbol: {SYMBOL} | Capital estimat: ~{total_capital:.1f}$ | Interval grid: {step:.1f}$ per nivel")
        log.info(f"📊 Grid: {GRID_LEVELS} nivele între {LOWER_PRICE} - {UPPER_PRICE} USDC")
        log.info(f"📏 Nivele: {[round_price(p, self.price_dec) for p in self.grid_prices]}")

    def get_grid_level(self, price):
        for i in range(len(self.grid_prices) - 1):
            if self.grid_prices[i] <= price < self.grid_prices[i + 1]:
                return i
        return None

    def check_pending_orders(self):
        """Verifică toate ordinele pending și actualizează DB dacă s-au executat."""
        for level, order_id in list(self.pending_orders.items()):
            status = check_order_status(self.client, order_id)
            if status == "filled":
                log.info(f"✅ Ordinul BUY {order_id} @ Nivel {level} s-a executat pe Binance!")
                db_confirm_position(level)
                del self.pending_orders[level]
            elif status == "canceled":
                log.warning(f"❌ Ordinul BUY {order_id} @ Nivel {level} anulat — șterg poziția")
                db_delete_position(level)
                del self.pending_orders[level]
                if level in self.positions:
                    del self.positions[level]

    def place_buy_order(self, price, level_index):
        qty     = round_qty(ORDER_AMOUNT, self.qty_dec)
        price_r = round_price(price, self.price_dec)
        try:
            order = self.client.create_order(
                symbol=SYMBOL, side="BUY", type="LIMIT",
                timeInForce="GTC", quantity=qty, price=str(price_r)
            )
            order_id = str(order["orderId"])
            self.positions[level_index]      = price_r
            self.pending_orders[level_index] = order_id
            self.trades_buy += 1
            db_save_position(level_index, price_r, order_id, status="pending")
            db_save_trade("BUY", level_index, price_r, qty, order_id=order_id)
            log.info(f"🟢 BUY LIMIT | Nivel {level_index} | Preț: {price_r}$ | Qty: {qty} | ID: {order_id} ⏳")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare BUY: {e}")
            return None

    def place_sell_order(self, sell_price, buy_price, level_index):
        # ── GARDĂ 0: SELL permis DOAR dacă BUY e confirmat filled în DB ──
        if level_index in self.pending_orders:
            log.warning(
                f"⏳ SELL BLOCAT | Nivel {level_index} | "
                f"BUY {self.pending_orders[level_index]} nu e confirmat încă!"
            )
            self.sells_blocked += 1
            return None

        # ── GARDĂ 1: Protecție împotriva pierderii ────────────────────────
        if sell_price <= buy_price:
            pierdere = (buy_price - sell_price) * round_qty(ORDER_AMOUNT, self.qty_dec)
            self.sells_blocked += 1
            log.warning(
                f"🚫 SELL BLOCAT (PIERDERE) | Nivel {level_index} | "
                f"Vânzare: {sell_price}$ ≤ Cumpărare: {buy_price}$ | "
                f"Pierdere evitată: -{pierdere:.4f} USDC"
            )
            return None

        # ── GARDĂ 2: Profit minim ─────────────────────────────────────────
        profit_pct = (sell_price - buy_price) / buy_price * 100
        if profit_pct < MIN_PROFIT_PCT:
            self.sells_blocked += 1
            log.warning(
                f"⛔ SELL BLOCAT (PROFIT INSUFICIENT) | Nivel {level_index} | "
                f"Profit: {profit_pct:.2f}% < minim {MIN_PROFIT_PCT}%"
            )
            return None

        qty          = round_qty(ORDER_AMOUNT, self.qty_dec)
        sell_price_r = round_price(sell_price, self.price_dec)
        profit_usdc  = (sell_price_r - buy_price) * qty

        try:
            order = self.client.create_order(
                symbol=SYMBOL, side="SELL", type="LIMIT",
                timeInForce="GTC", quantity=qty, price=str(sell_price_r)
            )
            order_id = str(order["orderId"])
            self.profit_total += profit_usdc
            self.trades_sell  += 1
            db_delete_position(level_index)
            db_save_trade("SELL", level_index, sell_price_r, qty, profit_usdc, profit_pct, order_id)
            del self.positions[level_index]
            log.info(
                f"🔴 SELL LIMIT | Nivel {level_index} | Vânzare: {sell_price_r}$ | "
                f"Cumpărare: {buy_price}$ | Profit: +{profit_usdc:.4f} USDC (+{profit_pct:.2f}%) | "
                f"ID: {order_id}"
            )
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

        filled_pos  = {k: v for k, v in self.positions.items() if k not in self.pending_orders}
        pending_pos = {k: v for k, v in self.positions.items() if k in self.pending_orders}
        filled_str  = " | ".join([f"Nivel {k} @ {v}$" for k, v in sorted(filled_pos.items())]) or "niciuna"
        pending_str = " | ".join([f"Nivel {k} @ {v}$" for k, v in sorted(pending_pos.items())]) or "niciuna"

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

    def run(self):
        log.info("🚀 Botul a pornit! Monitorizez prețul...")
        prev_level    = None
        check_counter = 0

        while True:
            try:
                current_price = get_current_price(self.client, SYMBOL)
                current_level = self.get_grid_level(current_price)

                if self.start_price is None:
                    self.start_price = current_price

                check_counter += 1

                # Verifică ordinele pending la fiecare 20 minute (40 × 30s)
                if check_counter % 40 == 0 and self.pending_orders:
                    self.check_pending_orders()

                # Dashboard la fiecare 60 minute (120 × 30s)
                if check_counter % 120 == 0:
                    self.print_dashboard(current_price)

                if current_level is None:
                    log.warning(f"⚠️  Prețul {current_price} în afara gridului ({LOWER_PRICE}-{UPPER_PRICE})")
                    time.sleep(CHECK_INTERVAL)
                    continue

                log.info(
                    f"💰 Preț: {current_price} | Nivel: {current_level} | "
                    f"Filled: {len(self.positions) - len(self.pending_orders)} | "
                    f"Pending: {len(self.pending_orders)}"
                )

                if prev_level is not None and current_level < prev_level:
                    buy_price = self.grid_prices[current_level]
                    if current_level not in self.positions:
                        self.place_buy_order(buy_price, current_level)

                elif prev_level is not None and current_level > prev_level:
                    sell_price = self.grid_prices[current_level]
                    if prev_level in self.positions:
                        buy_price = self.positions[prev_level]
                        self.place_sell_order(sell_price, buy_price, prev_level)

                prev_level = current_level
                time.sleep(CHECK_INTERVAL)

            except BinanceAPIException as e:
                log.error(f"❌ Eroare API Binance: {e}")
                if "-1003" in str(e):
                    log.warning("⏳ Rate limit / Ban detectat — aștept 5 minute...")
                    time.sleep(300)
                else:
                    time.sleep(30)
            except KeyboardInterrupt:
                log.info("🛑 Bot oprit de utilizator.")
                self.print_dashboard(get_current_price(self.client, SYMBOL))
                break
            except Exception as e:
                log.error(f"❌ Eroare neașteptată: {e}")
                time.sleep(30)

# ─────────────────────────────────────────────
#  PORNIRE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    db_init()
    client = create_client()
    test_trading_permission(client)
    bot = GridBot(client)
    bot.run()