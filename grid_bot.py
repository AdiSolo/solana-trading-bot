"""
Binance Testnet Grid Trading Bot — SOLUSDC (~50$)
==================================================
- Plasează ordine de cumpărare și vânzare la intervale fixe (grid)
- VINDE DOAR dacă prețul de vânzare > prețul de cumpărare (profit garantat)
- Niciodată nu vinde în pierdere — pozițiile rămân deschise până la profit
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
GRID_LEVELS    = 5
ORDER_AMOUNT   = 0.1
MIN_PROFIT_PCT = 0.2
CHECK_INTERVAL = 10

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
    """Returnează o conexiune la baza de date."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def db_init():
    """Creează tabelele dacă nu există."""
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
                    quantity   NUMERIC
                );
            """)
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
    """Încarcă pozițiile deschise din baza de date la pornire."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT level, buy_price FROM positions WHERE symbol = %s", (SYMBOL,))
            rows = cur.fetchall()
    positions = {row["level"]: float(row["buy_price"]) for row in rows}
    if positions:
        log.info(f"📂 Poziții încărcate din Supabase: {positions}")
    else:
        log.info("📂 Nicio poziție salvată anterior — start curat")
    return positions

def db_save_position(level, buy_price):
    """Salvează o poziție nouă în baza de date."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO positions (level, buy_price, symbol, quantity)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (level) DO UPDATE
                SET buy_price = EXCLUDED.buy_price,
                    updated_at = NOW()
            """, (level, buy_price, SYMBOL, ORDER_AMOUNT))
        conn.commit()
    log.info(f"💾 [DB] Poziție salvată → Nivel {level} @ {buy_price}$")

def db_delete_position(level):
    """Șterge o poziție din baza de date după vânzare."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM positions WHERE level = %s AND symbol = %s", (level, SYMBOL))
        conn.commit()
    log.info(f"🗑️  [DB] Poziție ștearsă → Nivel {level}")

def db_save_trade(trade_type, level, price, quantity, profit_usdc=0, profit_pct=0, order_id=""):
    """Salvează o tranzacție în istoricul din baza de date."""
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

        # Încarcă pozițiile salvate din Supabase (supraviețuiesc repornirilor!)
        self.positions = db_load_positions()

        # Statistici sesiune curentă
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

    def place_buy_order(self, price, level_index):
        qty     = round_qty(ORDER_AMOUNT, self.qty_dec)
        price_r = round_price(price, self.price_dec)
        try:
            order = self.client.create_order(
                symbol=SYMBOL, side="BUY", type="LIMIT",
                timeInForce="GTC", quantity=qty, price=str(price_r)
            )
            self.positions[level_index] = price_r
            self.trades_buy += 1
            # Salvează în Supabase
            db_save_position(level_index, price_r)
            db_save_trade("BUY", level_index, price_r, qty, order_id=str(order["orderId"]))
            log.info(f"🟢 BUY | Nivel {level_index} | Preț: {price_r}$ | Qty: {qty} | ID: {order['orderId']}")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare BUY: {e}")
            return None

    def place_sell_order(self, sell_price, buy_price, level_index):
        # ── GARDĂ 1: Protecție absolută împotriva pierderii ──────────────
        if sell_price <= buy_price:
            pierdere = (buy_price - sell_price) * round_qty(ORDER_AMOUNT, self.qty_dec)
            self.sells_blocked += 1
            log.warning(
                f"🚫 SELL BLOCAT (PIERDERE) | Nivel {level_index} | "
                f"Vânzare: {sell_price}$ ≤ Cumpărare: {buy_price}$ | "
                f"Pierdere evitată: -{pierdere:.4f} USDC | Poziție menținută în Supabase."
            )
            return None

        # ── GARDĂ 2: Profit minim pentru comisioane ───────────────────────
        profit_pct = (sell_price - buy_price) / buy_price * 100
        if profit_pct < MIN_PROFIT_PCT:
            self.sells_blocked += 1
            log.warning(
                f"⛔ SELL BLOCAT (PROFIT INSUFICIENT) | Nivel {level_index} | "
                f"Profit: {profit_pct:.2f}% < minim {MIN_PROFIT_PCT}% | Așteptăm..."
            )
            return None

        # ── Plasăm ordinul ────────────────────────────────────────────────
        qty          = round_qty(ORDER_AMOUNT, self.qty_dec)
        sell_price_r = round_price(sell_price, self.price_dec)
        profit_usdc  = (sell_price_r - buy_price) * qty

        try:
            order = self.client.create_order(
                symbol=SYMBOL, side="SELL", type="LIMIT",
                timeInForce="GTC", quantity=qty, price=str(sell_price_r)
            )
            self.profit_total += profit_usdc
            self.trades_sell  += 1
            # Actualizează Supabase
            db_delete_position(level_index)
            db_save_trade("SELL", level_index, sell_price_r, qty, profit_usdc, profit_pct, str(order["orderId"]))
            del self.positions[level_index]
            log.info(
                f"🔴 SELL | Nivel {level_index} | Vânzare: {sell_price_r}$ | "
                f"Cumpărare: {buy_price}$ | Profit: +{profit_usdc:.4f} USDC (+{profit_pct:.2f}%) | "
                f"ID: {order['orderId']}"
            )
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare SELL: {e}")
            return None

    def get_balance(self):
        try:
            account = self.client.get_account()
            USDC = next((float(b["free"]) for b in account["balances"] if b["asset"] == "USDC"), 0.0)
            sol  = next((float(b["free"]) for b in account["balances"] if b["asset"] == "SOL"),  0.0)
            return USDC, sol
        except Exception:
            return 0.0, 0.0

    def print_dashboard(self, current_price):
        USDC, sol   = self.get_balance()
        sol_value   = sol * current_price
        total_value = USDC + sol_value
        uptime      = datetime.now() - self.start_time
        hours, rem  = divmod(int(uptime.total_seconds()), 3600)
        minutes     = rem // 60

        price_change = ""
        if self.start_price:
            pct   = (current_price - self.start_price) / self.start_price * 100
            arrow = "📈" if pct >= 0 else "📉"
            price_change = f"{arrow} {pct:+.2f}% față de start"

        open_pos = " | ".join([f"Nivel {k} @ {v}$" for k, v in sorted(self.positions.items())]) or "niciuna"

        print("")
        print("═" * 62)
        print(f"  📊  GRID BOT DASHBOARD  —  {datetime.now().strftime('%H:%M:%S')}")
        print("═" * 62)
        print(f"  💹  SOL preț curent   : {current_price:.2f} USDC  {price_change}")
        print("─" * 62)
        print(f"  💵  Sold USDC liber   : {USDC:.2f} $")
        print(f"  🪙  Sold SOL liber    : {sol:.4f} SOL  (≈ {sol_value:.2f} $)")
        print(f"  💰  Valoare totală    : {total_value:.2f} $")
        print("─" * 62)
        print(f"  ✅  Profit realizat   : +{self.profit_total:.4f} USDC")
        print(f"  🟢  Cumpărări         : {self.trades_buy}")
        print(f"  🔴  Vânzări reușite   : {self.trades_sell}")
        print(f"  🚫  Vânzări blocate   : {self.sells_blocked}  (pierdere evitată)")
        print("─" * 62)
        print(f"  📌  Poziții (RAM+DB)  : {open_pos}")
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
                if check_counter % 6 == 0:
                    self.print_dashboard(current_price)

                if current_level is None:
                    log.warning(f"⚠️  Prețul {current_price} în afara gridului ({LOWER_PRICE}-{UPPER_PRICE})")
                    time.sleep(CHECK_INTERVAL)
                    continue

                log.info(f"💰 Preț: {current_price} | Nivel: {current_level} | Poziții active: {len(self.positions)}")

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
    bot = GridBot(client)
    bot.run()
