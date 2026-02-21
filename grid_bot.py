"""
Binance Testnet Grid Trading Bot
=================================
- Plasează ordine de cumpărare și vânzare la intervale fixe (grid)
- VINDE DOAR dacă prețul de vânzare > prețul de cumpărare (profit garantat)
- Rulează pe Binance Spot Testnet

Instalare:
    pip install python-binance

Configurare Testnet:
    1. Mergi la https://testnet.binance.vision/
    2. Loghează-te cu GitHub
    3. Generează API Key și Secret
    4. Adaugă-le mai jos sau în fișierul .env
"""

import time
import os
from datetime import datetime
import math
import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ─────────────────────────────────────────────
#  CONFIGURARE - Modifică valorile de mai jos
# ─────────────────────────────────────────────

# Cheile se citesc din variabile de mediu (setate pe Railway)
# Nu pune niciodată cheile direct în cod!
API_KEY    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

if not API_KEY or not API_SECRET:
    raise ValueError("❌ Lipsesc variabilele de mediu BINANCE_API_KEY și BINANCE_API_SECRET!")

SYMBOL          = "SOLUSDT"   # Perechea de tranzacționare
LOWER_PRICE     = 70.0        # Prețul minim al gridului (USDT) — ~-17% față de ~84$
UPPER_PRICE     = 100.0       # Prețul maxim al gridului (USDT) — ~+19% față de ~84$
GRID_LEVELS     = 5           # 5 nivele = interval de 6$ fiecare
ORDER_AMOUNT    = 0.1         # 0.1 SOL per ordin (~8.4$ la 84$) → total ~42$ + rezervă
MIN_PROFIT_PCT  = 0.2         # Profit minim 0.2% (acoperă comisionul Binance de 0.1% x2)
CHECK_INTERVAL  = 10          # Secunde între verificări

# ─── Notă buget ─────────────────────────────────────────
# 5 nivele × 0.1 SOL × 84$ ≈ 42$ capital activ
# Păstrează ~8$ rezervă în cont pentru fluctuații
# ────────────────────────────────────────────────────────

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("grid_bot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CONECTARE BINANCE TESTNET
# ─────────────────────────────────────────────

def create_client():
    client = Client(API_KEY, API_SECRET, testnet=True)
    # Testnet endpoints
    client.API_URL = "https://testnet.binance.vision/api"
    log.info("✅ Conectat la Binance Testnet")
    return client

# ─────────────────────────────────────────────
#  FUNCȚII UTILITARE
# ─────────────────────────────────────────────

def get_current_price(client, symbol):
    """Returnează prețul curent al unui simbol."""
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker["price"])

def get_symbol_info(client, symbol):
    """Returnează informații despre simbol (precizie prețuri/cantități)."""
    info = client.get_symbol_info(symbol)
    price_filter  = next(f for f in info["filters"] if f["filterType"] == "PRICE_FILTER")
    lot_filter    = next(f for f in info["filters"] if f["filterType"] == "LOT_SIZE")
    tick_size     = float(price_filter["tickSize"])
    step_size     = float(lot_filter["stepSize"])
    price_decimals = int(round(-math.log10(tick_size)))
    qty_decimals   = int(round(-math.log10(step_size)))
    return price_decimals, qty_decimals

def round_price(price, decimals):
    return round(price, decimals)

def round_qty(qty, decimals):
    return round(qty, decimals)

def calculate_grid_levels(lower, upper, levels):
    """Calculează prețurile fiecărui nivel al gridului."""
    step = (upper - lower) / levels
    return [lower + i * step for i in range(levels + 1)]

# ─────────────────────────────────────────────
#  LOGICA PRINCIPALĂ A BOTULUI
# ─────────────────────────────────────────────

class GridBot:
    def __init__(self, client):
        self.client = client
        self.price_dec, self.qty_dec = get_symbol_info(client, SYMBOL)
        self.grid_prices = calculate_grid_levels(LOWER_PRICE, UPPER_PRICE, GRID_LEVELS)
        
        # Dicționar: nivel_grid -> prețul la care am cumpărat
        self.positions = {}  # {grid_index: buy_price}

        # ── Tracking profit & statistici ────────────────────────────────────
        self.profit_total  = 0.0   # Profit cumulat din vânzări (USDT)
        self.trades_buy    = 0     # Număr cumpărări
        self.trades_sell   = 0     # Număr vânzări reușite
        self.sells_blocked = 0     # Vânzări blocate (ar fi fost în pierdere)
        self.start_time    = datetime.now()
        self.start_price   = None  # Prețul SOL la pornire
        
        step = (UPPER_PRICE - LOWER_PRICE) / GRID_LEVELS
        total_capital = ORDER_AMOUNT * LOWER_PRICE * GRID_LEVELS
        log.info(f"💎 Simbol: {SYMBOL} | Capital estimat necesar: ~{total_capital:.1f}$ | Interval grid: {step:.1f}$ per nivel")
        log.info(f"📊 Grid configurat: {GRID_LEVELS} nivele între {LOWER_PRICE} - {UPPER_PRICE} USDT")
        log.info(f"📏 Nivele grid: {[round_price(p, self.price_dec) for p in self.grid_prices]}")

    def get_grid_level(self, price):
        """Returnează indexul nivelului de grid pentru prețul dat."""
        for i in range(len(self.grid_prices) - 1):
            if self.grid_prices[i] <= price < self.grid_prices[i + 1]:
                return i
        return None

    def place_buy_order(self, price, level_index):
        """Plasează un ordin de cumpărare la nivelul dat."""
        qty  = round_qty(ORDER_AMOUNT, self.qty_dec)
        price_r = round_price(price, self.price_dec)
        
        try:
            order = self.client.create_order(
                symbol=SYMBOL,
                side="BUY",
                type="LIMIT",
                timeInForce="GTC",
                quantity=qty,
                price=str(price_r)
            )
            self.positions[level_index] = price_r
            self.trades_buy += 1
            log.info(f"🟢 BUY  ordin plasat | Nivel {level_index} | Preț: {price_r} | ID: {order['orderId']}")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare BUY: {e}")
            return None

    def place_sell_order(self, sell_price, buy_price, level_index):
        """
        Plasează un ordin de vânzare STRICT NUMAI dacă:
          1. sell_price > buy_price  (niciodată în pierdere)
          2. Profitul procentual >= MIN_PROFIT_PCT (acoperă fee-urile)

        Dacă una din condiții nu e îndeplinită, ordinul este BLOCAT complet.
        Poziția rămâne deschisă și va fi re-evaluată la următoarea mișcare.
        """

        # ── GARDĂ 1: Protecție absolută împotriva pierderii ──────────────────
        if sell_price <= buy_price:
            pierdere = (buy_price - sell_price) * round_qty(ORDER_AMOUNT, self.qty_dec)
            self.sells_blocked += 1
            log.warning(
                f"🚫 SELL BLOCAT (PIERDERE) | Nivel {level_index} | "
                f"Preț vânzare: {sell_price} ≤ Preț cumpărare: {buy_price} | "
                f"Pierdere evitată: -{pierdere:.4f} USDT | Poziție menținută."
            )
            return None

        # ── GARDĂ 2: Profit minim pentru a acoperi comisioanele ──────────────
        profit_pct = (sell_price - buy_price) / buy_price * 100
        if profit_pct < MIN_PROFIT_PCT:
            self.sells_blocked += 1
            log.warning(
                f"⛔ SELL BLOCAT (PROFIT INSUFICIENT) | Nivel {level_index} | "
                f"Profit estimat {profit_pct:.2f}% < minim {MIN_PROFIT_PCT}% | "
                f"Așteptăm un preț mai bun..."
            )
            return None

        # ── Toate verificările trecute → plasăm ordinul ──────────────────────
        qty          = round_qty(ORDER_AMOUNT, self.qty_dec)
        sell_price_r = round_price(sell_price, self.price_dec)
        profit_usdt  = (sell_price_r - buy_price) * qty

        try:
            order = self.client.create_order(
                symbol=SYMBOL,
                side="SELL",
                type="LIMIT",
                timeInForce="GTC",
                quantity=qty,
                price=str(sell_price_r)
            )
            log.info(
                f"🔴 SELL ordin plasat | Nivel {level_index} | "
                f"Preț vânzare: {sell_price_r} | Preț cumpărare: {buy_price} | "
                f"Profit: +{profit_usdt:.4f} USDT (+{profit_pct:.2f}%) | "
                f"ID: {order['orderId']}"
            )
            self.profit_total += profit_usdt
            self.trades_sell += 1
            del self.positions[level_index]
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Eroare SELL: {e}")
            return None

    def get_balance(self):
        """Returnează soldul USDT și SOL din cont."""
        try:
            account = self.client.get_account()
            usdt = next((float(b["free"]) for b in account["balances"] if b["asset"] == "USDT"), 0.0)
            sol  = next((float(b["free"]) for b in account["balances"] if b["asset"] == "SOL"),  0.0)
            return usdt, sol
        except Exception:
            return 0.0, 0.0

    def print_dashboard(self, current_price):
        """Afișează un dashboard cu sold, profit și statistici."""
        usdt, sol = self.get_balance()
        sol_value    = sol * current_price
        total_value  = usdt + sol_value
        uptime       = datetime.now() - self.start_time
        hours, rem   = divmod(int(uptime.total_seconds()), 3600)
        minutes      = rem // 60

        # Profit față de prețul de start
        price_change = ""
        if self.start_price:
            pct = (current_price - self.start_price) / self.start_price * 100
            arrow = "📈" if pct >= 0 else "📉"
            price_change = f"{arrow} SOL de la start: {pct:+.2f}%"

        # Poziții deschise
        open_pos = ""
        if self.positions:
            pos_list = [f"Nivel {k} @ {v}$" for k, v in sorted(self.positions.items())]
            open_pos = " | ".join(pos_list)
        else:
            open_pos = "niciuna"

        print("")
        print("═" * 58)
        print(f"  📊  GRID BOT DASHBOARD  —  {datetime.now().strftime('%H:%M:%S')}")
        print("═" * 58)
        print(f"  💹  SOL preț curent   : {current_price:.2f} USDT")
        if self.start_price:
            print(f"  🏁  SOL la pornire    : {self.start_price:.2f} USDT  {price_change}")
        print("─" * 58)
        print(f"  💵  Sold USDT liber   : {usdt:.2f} $")
        print(f"  🪙  Sold SOL liber    : {sol:.4f} SOL  (≈ {sol_value:.2f} $)")
        print(f"  💰  Valoare totală    : {total_value:.2f} $")
        print("─" * 58)
        print(f"  ✅  Profit realizat   : +{self.profit_total:.4f} USDT")
        print(f"  🟢  Cumpărări         : {self.trades_buy}")
        print(f"  🔴  Vânzări reușite   : {self.trades_sell}")
        print(f"  🚫  Vânzări blocate   : {self.sells_blocked}  (pierdere evitată)")
        print("─" * 58)
        print(f"  📌  Poziții deschise  : {open_pos}")
        print(f"  ⏱️   Uptime            : {hours}h {minutes}m")
        print("═" * 58)
        print("")

    def run(self):
        log.info("🚀 Botul a pornit! Monitorizez prețul...")
        prev_level    = None
        check_counter = 0

        while True:
            try:
                current_price = get_current_price(self.client, SYMBOL)
                current_level = self.get_grid_level(current_price)

                # Salvează prețul de start o singură dată
                if self.start_price is None:
                    self.start_price = current_price

                # Afișează dashboard la fiecare 6 verificări (~60 secunde)
                check_counter += 1
                if check_counter % 6 == 0:
                    self.print_dashboard(current_price)

                if current_level is None:
                    log.warning(f"⚠️  Prețul {current_price} este în afara gridului ({LOWER_PRICE} - {UPPER_PRICE})")
                    time.sleep(CHECK_INTERVAL)
                    continue

                log.info(f"💰 Preț curent: {current_price} | Nivel grid: {current_level}")

                # Dacă am coborât un nivel → CUMPĂRĂM
                if prev_level is not None and current_level < prev_level:
                    buy_price = self.grid_prices[current_level]
                    if current_level not in self.positions:
                        self.place_buy_order(buy_price, current_level)

                # Dacă am urcat un nivel → VINDEM (doar dacă avem profit)
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
    client = create_client()
    bot = GridBot(client)
    bot.run()
