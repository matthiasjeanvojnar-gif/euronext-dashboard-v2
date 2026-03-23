"""
Euronext Market Activity Dashboard
Streamlit Cloud edition — fetches live delayed data on page load.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import time
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

MARKETS = {
    "Paris":     {"mic": "XPAR", "flag": "🇫🇷", "currency": "EUR"},
    "Amsterdam": {"mic": "XAMS", "flag": "🇳🇱", "currency": "EUR"},
    "Brussels":  {"mic": "XBRU", "flag": "🇧🇪", "currency": "EUR"},
    "Lisbon":    {"mic": "XLIS", "flag": "🇵🇹", "currency": "EUR"},
    "Milan":     {"mic": "XMIL", "flag": "🇮🇹", "currency": "EUR"},
    "Oslo":      {"mic": "XOSL", "flag": "🇳🇴", "currency": "NOK"},
    "Dublin":    {"mic": "XDUB", "flag": "🇮🇪", "currency": "EUR"},
}

BASE_URL = "https://live.euronext.com"
NOK_EUR_RATE = 0.085
PAGE_SIZE = 100
MAX_PAGES = 30
REQUEST_TIMEOUT = 20

ICB_SEED = {
    "FR0000120271": "Energy", "FR0000131104": "Financials",
    "NL0010273215": "Technology", "FR0000121014": "Consumer Discretionary",
    "FR0000121972": "Technology", "FR0000120578": "Industrials",
    "FR0000125338": "Consumer Discretionary", "FR0000120073": "Industrials",
    "FR0000133308": "Consumer Discretionary", "FR0000120321": "Consumer Discretionary",
    "NL0000235190": "Technology", "FR0000073272": "Technology",
    "FR0000125007": "Basic Materials", "FR0000124141": "Utilities",
    "FR0000130809": "Financials", "FR0000045072": "Financials",
    "NL0000009165": "Industrials", "NL0011821202": "Financials",
    "NL0012969182": "Technology", "NL0010773842": "Technology",
    "FR0000131906": "Consumer Staples", "FR0000120693": "Consumer Staples",
    "FR0000130577": "Financials", "FR0014003TT8": "Energy",
    "FR0000120644": "Consumer Staples", "FR0000051807": "Telecommunications",
    "FR0010307819": "Consumer Discretionary", "FR0000125486": "Consumer Discretionary",
    "IT0003128367": "Energy", "IT0003132476": "Financials",
    "IT0000072618": "Financials", "IT0005239360": "Telecommunications",
    "IT0003856405": "Technology", "NO0010096985": "Energy",
    "NO0005052605": "Energy", "NO0010031479": "Telecommunications",
    "NO0003054108": "Consumer Staples", "NO0003733800": "Industrials",
    "PTEDP0AM0009": "Utilities", "PTGAL0AM0009": "Consumer Staples",
    "PTJMT0AE0001": "Consumer Staples", "PTBCP0AM0015": "Financials",
    "IE00B4BNMY34": "Basic Materials", "IE0001827041": "Basic Materials",
    "IE00BDB6Q211": "Industrials", "FR0000120222": "Industrials",
    "FR0000121485": "Consumer Discretionary", "FR0000125585": "Consumer Discretionary",
    "FR0000120503": "Consumer Discretionary", "FR0000052292": "Health Care",
    "FR0000120628": "Industrials", "FR0012435121": "Health Care",
    "FR0000124711": "Consumer Discretionary", "FR0000121667": "Industrials",
    "NL0000009538": "Consumer Staples", "NL0011585146": "Financials",
    "FR0010220475": "Financials", "FR0000131757": "Industrials",
    "FR0013269123": "Industrials", "FR0010040865": "Industrials",
    "FR0000130650": "Consumer Staples", "IT0005366767": "Utilities",
    "IT0003796171": "Financials", "IT0000062072": "Financials",
    "IT0004781412": "Industrials", "IT0001233417": "Consumer Discretionary",
    "NO0010345853": "Financials", "NO0003078800": "Financials",
    "NO0010063308": "Energy", "BE0003565737": "Consumer Staples",
    "BE0974293251": "Financials", "BE0003739530": "Industrials",
    "BE0003793107": "Industrials", "BE0974264930": "Health Care",
    "LU1598757687": "Industrials", "NL0015000IY2": "Consumer Discretionary",
    "FR0000127771": "Consumer Discretionary",
}


# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def _parse_number(val: str | None) -> float:
    if not val or val.strip() in ("", "-", "N/A", "n/a", "--"):
        return 0.0
    val = val.strip().replace("\xa0", "").replace(" ", "")
    negative = val.startswith("-")
    val = val.lstrip("-+")
    if "," in val and "." in val:
        if val.rindex(",") > val.rindex("."):
            val = val.replace(".", "").replace(",", ".")
        else:
            val = val.replace(",", "")
    elif "," in val:
        parts = val.split(",")
        if len(parts) == 2 and len(parts[1]) <= 3:
            val = val.replace(",", ".")
        else:
            val = val.replace(",", "")
    try:
        result = float(val)
        return -result if negative else result
    except ValueError:
        return 0.0


def _build_payload(mic: str, start: int = 0, length: int = PAGE_SIZE) -> dict:
    payload = {
        "draw": "1",
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "5",
        "order[0][dir]": "desc",
        "args[initialLetter]": "",
        "args[fe_type]": "csv",
        "args[fe_layout]": "col8",
        "args[fe_page]": "tp_allequities",
        "args[fe_market]": mic,
    }
    for i in range(6):
        payload[f"columns[{i}][data]"] = str(i)
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = "true"
        payload[f"columns[{i}][orderable]"] = "false"
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = "false"
    return payload


def _parse_row(row: list, market_name: str, mic: str, currency: str) -> dict | None:
    try:
        if not isinstance(row, list) or len(row) < 6:
            return None

        def strip_html(html_str):
            s = BeautifulSoup(str(html_str), "html.parser")
            return s.get_text(strip=True)

        def extract_link_text(html_str):
            s = BeautifulSoup(str(html_str), "html.parser")
            a = s.find("a")
            return a.get_text(strip=True) if a else s.get_text(strip=True)

        name = extract_link_text(row[0])
        isin = strip_html(row[1]) if len(row) > 1 else ""
        symbol = strip_html(row[2]) if len(row) > 2 else ""
        last_price = _parse_number(strip_html(row[4])) if len(row) > 4 else 0
        change_pct = _parse_number(strip_html(row[5]).replace("%", "")) if len(row) > 5 else 0
        volume = _parse_number(strip_html(row[6])) if len(row) > 6 else 0
        turnover = _parse_number(strip_html(row[7])) if len(row) > 7 else 0

        if turnover == 0 and last_price > 0 and volume > 0:
            turnover = last_price * volume

        turnover_eur = turnover * NOK_EUR_RATE if currency == "NOK" else turnover

        sector = ICB_SEED.get(isin, "Other")

        return {
            "name": name, "isin": isin, "symbol": symbol,
            "market": market_name, "mic": mic, "currency": currency,
            "last_price": last_price, "change_pct": change_pct,
            "volume": volume, "turnover": turnover_eur, "sector": sector,
        }
    except Exception:
        return None


@st.cache_data(ttl=600, show_spinner=False)
def fetch_market_data(market_name: str, mic: str, currency: str) -> list[dict]:
    """Fetch all equities for one Euronext market. Cached 10 min."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{BASE_URL}/en/products/equities/list",
        "Origin": BASE_URL,
    })

    try:
        session.get(f"{BASE_URL}/en/products/equities/list", timeout=REQUEST_TIMEOUT)
    except Exception:
        pass

    instruments = []
    for page in range(MAX_PAGES):
        try:
            payload = _build_payload(mic, start=page * PAGE_SIZE)
            resp = session.post(
                f"{BASE_URL}/en/pd_es/data/stocks",
                data=payload, timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            rows = data.get("data", [])
            if not rows:
                break
            for row in rows:
                inst = _parse_row(row, market_name, mic, currency)
                if inst and inst["name"]:
                    instruments.append(inst)
            total = data.get("recordsTotal", 0)
            if (page + 1) * PAGE_SIZE >= total:
                break
        except Exception:
            break
        time.sleep(0.3)

    return instruments


def fetch_all() -> pd.DataFrame:
    """Fetch all markets and return a combined DataFrame."""
    all_instruments = []
    statuses = {}
    progress = st.progress(0, text="Connecting to Euronext Live…")

    market_list = list(MARKETS.items())
    for i, (name, cfg) in enumerate(market_list):
        progress.progress(
            (i) / len(market_list),
            text=f"Fetching {cfg['flag']} {name}…",
        )
        try:
            instruments = fetch_market_data(name, cfg["mic"], cfg["currency"])
            all_instruments.extend(instruments)
            statuses[name] = len(instruments)
        except Exception as e:
            statuses[name] = 0

    progress.progress(1.0, text="Data loaded.")
    time.sleep(0.4)
    progress.empty()

    if all_instruments:
        df = pd.DataFrame(all_instruments)
    else:
        df = pd.DataFrame()

    return df, statuses


# ═══════════════════════════════════════════════════════════════
# FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════

def fmt_eur(val: float) -> str:
    if abs(val) >= 1e9:
        return f"€{val / 1e9:,.2f}B"
    if abs(val) >= 1e6:
        return f"€{val / 1e6:,.1f}M"
    if abs(val) >= 1e3:
        return f"€{val / 1e3:,.0f}K"
    return f"€{val:,.0f}"


def fmt_vol(val: float) -> str:
    if abs(val) >= 1e9:
        return f"{val / 1e9:,.2f}B"
    if abs(val) >= 1e6:
        return f"{val / 1e6:,.1f}M"
    if abs(val) >= 1e3:
        return f"{val / 1e3:,.0f}K"
    return f"{val:,.0f}"


def fmt_pct(val: float) -> str:
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


# ═══════════════════════════════════════════════════════════════
# PLOTLY THEME
# ═══════════════════════════════════════════════════════════════

PALETTE = {
    "bg": "rgba(0,0,0,0)",
    "grid": "rgba(148,163,184,0.06)",
    "text": "#94a3b8",
    "text_bright": "#e2e8f0",
    "blue": "#6ea8fe",
    "green": "#63e6be",
    "red": "#ff8787",
    "amber": "#ffd43b",
    "muted": "#475569",
    "series": ["#6ea8fe", "#63e6be", "#ffd43b", "#ff8787", "#c084fc", "#f472b6", "#38bdf8"],
}


def apply_theme(fig, height=380):
    fig.update_layout(
        paper_bgcolor=PALETTE["bg"],
        plot_bgcolor=PALETTE["bg"],
        font=dict(family="'Söhne', 'Helvetica Neue', Helvetica, sans-serif", color=PALETTE["text"], size=12),
        margin=dict(l=0, r=0, t=36, b=0),
        height=height,
        xaxis=dict(gridcolor=PALETTE["grid"], zerolinecolor=PALETTE["grid"], showline=False),
        yaxis=dict(gridcolor=PALETTE["grid"], zerolinecolor=PALETTE["grid"], showline=False),
        colorway=PALETTE["series"],
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
            font=dict(size=11, color=PALETTE["text"]),
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="left", x=0,
        ),
        hoverlabel=dict(
            bgcolor="#1e293b",
            bordercolor="#334155",
            font_size=12,
            font_color="#e2e8f0",
        ),
    )
    return fig


# ═══════════════════════════════════════════════════════════════
# PAGE SETUP & CSS
# ═══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Euronext Activity",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    /* ── Foundations ────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

    :root {
        --c-bg:       #0b0f19;
        --c-surface:  #111827;
        --c-surface2: #161f30;
        --c-border:   rgba(148,163,184,0.08);
        --c-border-h: rgba(148,163,184,0.15);
        --c-text:     #e2e8f0;
        --c-text-2:   #94a3b8;
        --c-text-3:   #64748b;
        --c-blue:     #6ea8fe;
        --c-green:    #63e6be;
        --c-red:      #ff8787;
        --c-amber:    #ffd43b;
        --radius:     10px;
    }

    html, body, [data-testid="stAppViewContainer"],
    [data-testid="stApp"] {
        font-family: 'Instrument Sans', 'Helvetica Neue', Helvetica, sans-serif;
    }

    .main .block-container {
        padding: 1.4rem 2rem 2rem 2rem;
        max-width: 1360px;
    }

    /* Kill Streamlit chrome */
    #MainMenu, footer, header[data-testid="stHeader"] { display: none !important; }

    /* ── Header bar ────────────────────────────────── */
    .hdr {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        margin-bottom: 1.6rem;
        flex-wrap: wrap;
        gap: 8px;
    }
    .hdr-title {
        font-size: 1.35rem;
        font-weight: 700;
        color: var(--c-text);
        letter-spacing: -0.02em;
    }
    .hdr-title span {
        color: var(--c-blue);
    }
    .hdr-meta {
        font-size: 0.76rem;
        color: var(--c-text-3);
        font-family: 'IBM Plex Mono', monospace;
    }

    /* ── KPI row ───────────────────────────────────── */
    .kpi-row {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 12px;
        margin-bottom: 1.5rem;
    }
    .kpi {
        background: var(--c-surface);
        border: 1px solid var(--c-border);
        border-radius: var(--radius);
        padding: 18px 20px 16px;
        transition: border-color 0.15s;
    }
    .kpi:hover { border-color: var(--c-border-h); }
    .kpi-label {
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        color: var(--c-text-3);
        margin-bottom: 6px;
    }
    .kpi-val {
        font-size: 1.55rem;
        font-weight: 700;
        color: var(--c-text);
        letter-spacing: -0.02em;
        line-height: 1.15;
    }
    .kpi-sub {
        font-size: 0.72rem;
        font-weight: 500;
        margin-top: 3px;
        font-family: 'IBM Plex Mono', monospace;
    }
    .kpi-sub.up   { color: var(--c-green); }
    .kpi-sub.down { color: var(--c-red); }
    .kpi-sub.flat { color: var(--c-text-3); }

    /* ── Section label ─────────────────────────────── */
    .sec {
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: var(--c-text-3);
        margin: 2rem 0 0.85rem 0;
        padding-bottom: 6px;
        border-bottom: 1px solid var(--c-border);
    }

    /* ── Market cards grid ─────────────────────────── */
    .mkt-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
        gap: 12px;
        margin-bottom: 0.5rem;
    }
    .mkt {
        background: var(--c-surface);
        border: 1px solid var(--c-border);
        border-radius: var(--radius);
        padding: 16px 18px;
        transition: border-color 0.15s;
    }
    .mkt:hover { border-color: var(--c-border-h); }

    .mkt-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 12px;
    }
    .mkt-name {
        font-size: 0.88rem;
        font-weight: 600;
        color: var(--c-text);
    }
    .mkt-count {
        font-size: 0.68rem;
        color: var(--c-text-3);
        font-family: 'IBM Plex Mono', monospace;
    }
    .mkt-row {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        padding: 3px 0;
    }
    .mkt-lbl {
        font-size: 0.7rem;
        color: var(--c-text-3);
    }
    .mkt-v {
        font-size: 0.82rem;
        font-weight: 600;
        color: var(--c-text-2);
        font-family: 'IBM Plex Mono', monospace;
    }
    .bar-track {
        height: 3px;
        background: var(--c-border);
        border-radius: 2px;
        margin-top: 10px;
        overflow: hidden;
    }
    .bar-fill {
        height: 100%;
        border-radius: 2px;
        background: linear-gradient(90deg, var(--c-blue), #c084fc);
        transition: width 0.6s ease;
    }

    /* ── Empty state ───────────────────────────────── */
    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        color: var(--c-text-3);
    }
    .empty-state h3 {
        font-size: 1.15rem;
        font-weight: 600;
        color: var(--c-text-2);
        margin-bottom: 0.5rem;
    }
    .empty-state p {
        font-size: 0.85rem;
        max-width: 420px;
        margin: 0 auto;
        line-height: 1.6;
    }

    /* ── Tables ────────────────────────────────────── */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--c-border) !important;
        border-radius: var(--radius) !important;
        overflow: hidden;
    }

    /* ── Sidebar ───────────────────────────────────── */
    [data-testid="stSidebar"] {
        background: #0a0e17;
    }
    [data-testid="stSidebar"] [data-testid="stMarkdown"] p {
        font-size: 0.82rem;
    }

    /* ── Tabs ──────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        font-size: 0.8rem;
        font-weight: 500;
        letter-spacing: 0.01em;
        padding: 10px 18px;
    }
    .stTabs [data-baseweb="tab-highlight"] {
        background-color: var(--c-blue);
    }

    /* ── Misc polish ──────────────────────────────── */
    hr { border-color: var(--c-border); }
    .stButton > button {
        border-radius: 8px;
        font-weight: 500;
        font-size: 0.78rem;
        letter-spacing: 0.01em;
    }
    .stProgress > div > div {
        background-color: var(--c-blue) !important;
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════

if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
    st.session_state.statuses = {}
    st.session_state.last_fetch = None


def load_data():
    df, statuses = fetch_all()
    st.session_state.df = df
    st.session_state.statuses = statuses
    st.session_state.last_fetch = datetime.now(timezone.utc)


# ── Header ──────────────────────────────────────────────────
col_h1, col_h2 = st.columns([5, 1])
with col_h1:
    ts_str = ""
    if st.session_state.last_fetch:
        ts_str = st.session_state.last_fetch.strftime("%H:%M:%S UTC · %d %b %Y")
    st.markdown(f"""
    <div class="hdr">
        <div class="hdr-title">◆ Euronext <span>Activity Monitor</span></div>
        <div class="hdr-meta">{ts_str}</div>
    </div>
    """, unsafe_allow_html=True)
with col_h2:
    if st.button("⟳ Refresh data", use_container_width=True, type="primary"):
        st.cache_data.clear()
        load_data()
        st.rerun()

# Auto-load on first visit
if st.session_state.df.empty and st.session_state.last_fetch is None:
    load_data()
    st.rerun()

df = st.session_state.df
statuses = st.session_state.statuses

# ═══════════════════════════════════════════════════════════════
# EMPTY STATE
# ═══════════════════════════════════════════════════════════════

if df.empty:
    st.markdown("""
    <div class="empty-state">
        <h3>No data available right now</h3>
        <p>
            The Euronext Live endpoints could not be reached at the moment.
            This may happen outside market hours or if the service is temporarily
            unavailable. Try refreshing in a few minutes.
        </p>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ═══════════════════════════════════════════════════════════════
# COMPUTE AGGREGATES
# ═══════════════════════════════════════════════════════════════

total_turnover = df["turnover"].sum()
total_volume = df["volume"].sum()
total_instruments = len(df)
markets_active = df["market"].nunique()

# Weighted average change
df["weight"] = df["turnover"] / total_turnover if total_turnover > 0 else 0
weighted_chg = (df["change_pct"] * df["weight"]).sum()

# Per-market
mkt_agg = df.groupby("market").agg(
    turnover=("turnover", "sum"),
    volume=("volume", "sum"),
    count=("isin", "count"),
    avg_chg=("change_pct", "mean"),
).reset_index().sort_values("turnover", ascending=False)
mkt_agg["share"] = (mkt_agg["turnover"] / total_turnover * 100) if total_turnover > 0 else 0

# Per-sector
sec_agg = df.groupby("sector").agg(
    turnover=("turnover", "sum"),
    volume=("volume", "sum"),
    count=("isin", "count"),
    avg_chg=("change_pct", "mean"),
).reset_index().sort_values("turnover", ascending=False)
sec_agg["share"] = (sec_agg["turnover"] / total_turnover * 100) if total_turnover > 0 else 0


# ═══════════════════════════════════════════════════════════════
# 1 · KPI CARDS
# ═══════════════════════════════════════════════════════════════

chg_class = "up" if weighted_chg > 0.01 else ("down" if weighted_chg < -0.01 else "flat")

st.markdown(f"""
<div class="kpi-row">
    <div class="kpi">
        <div class="kpi-label">Total Turnover</div>
        <div class="kpi-val">{fmt_eur(total_turnover)}</div>
        <div class="kpi-sub {chg_class}">Weighted avg chg {fmt_pct(weighted_chg)}</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Total Volume</div>
        <div class="kpi-val">{fmt_vol(total_volume)}</div>
        <div class="kpi-sub flat">{total_instruments:,} securities</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Active Securities</div>
        <div class="kpi-val">{total_instruments:,}</div>
        <div class="kpi-sub flat">across {markets_active} markets</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Markets Reporting</div>
        <div class="kpi-val">{markets_active}<span style="font-size:0.85rem;font-weight:400;color:var(--c-text-3)"> / {len(MARKETS)}</span></div>
        <div class="kpi-sub flat">Delayed ~15 min</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# 2 · MARKET BREAKDOWN
# ═══════════════════════════════════════════════════════════════

st.markdown('<div class="sec">Market breakdown</div>', unsafe_allow_html=True)

cards = '<div class="mkt-grid">'
for _, row in mkt_agg.iterrows():
    mkt_name = row["market"]
    flag = MARKETS.get(mkt_name, {}).get("flag", "🏛️")
    share = row["share"]
    chg = row["avg_chg"]
    chg_c = "up" if chg > 0.01 else ("down" if chg < -0.01 else "flat")

    cards += f"""
    <div class="mkt">
        <div class="mkt-head">
            <div class="mkt-name">{flag} {mkt_name}</div>
            <div class="mkt-count">{int(row['count']):,} sec</div>
        </div>
        <div class="mkt-row">
            <span class="mkt-lbl">Turnover</span>
            <span class="mkt-v">{fmt_eur(row['turnover'])}</span>
        </div>
        <div class="mkt-row">
            <span class="mkt-lbl">Volume</span>
            <span class="mkt-v">{fmt_vol(row['volume'])}</span>
        </div>
        <div class="mkt-row">
            <span class="mkt-lbl">Share</span>
            <span class="mkt-v">{share:.1f}%</span>
        </div>
        <div class="mkt-row">
            <span class="mkt-lbl">Avg chg</span>
            <span class="mkt-v kpi-sub {chg_c}">{fmt_pct(chg)}</span>
        </div>
        <div class="bar-track"><div class="bar-fill" style="width:{min(share, 100):.1f}%"></div></div>
    </div>
    """

# Add cards for markets that returned zero data
for mkt_name, cfg in MARKETS.items():
    if mkt_name not in mkt_agg["market"].values:
        cards += f"""
        <div class="mkt" style="opacity:0.4">
            <div class="mkt-head">
                <div class="mkt-name">{cfg['flag']} {mkt_name}</div>
                <div class="mkt-count">—</div>
            </div>
            <div class="mkt-row">
                <span class="mkt-lbl">Status</span>
                <span class="mkt-v" style="color:var(--c-text-3)">No data</span>
            </div>
            <div class="bar-track"><div class="bar-fill" style="width:0%"></div></div>
        </div>
        """

cards += '</div>'
st.markdown(cards, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════

tab_charts, tab_sectors, tab_tables = st.tabs(["Charts", "Sectors", "Tables"])


# ── Tab 1: Charts ───────────────────────────────────────────
with tab_charts:
    st.markdown('<div class="sec">Turnover distribution</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(
            mkt_agg, x="market", y="turnover",
            color="market", text_auto=".2s",
            labels={"turnover": "Turnover (€)", "market": ""},
        )
        fig.update_traces(textposition="outside", textfont_size=11, marker_line_width=0)
        fig.update_layout(showlegend=False, title_text="Turnover by Market", title_font_size=14)
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True, key="bar_mkt")

    with c2:
        fig = px.pie(
            mkt_agg, values="turnover", names="market",
            hole=0.55,
        )
        fig.update_traces(
            textposition="inside", textinfo="percent+label",
            textfont_size=11,
            marker=dict(line=dict(color="#0b0f19", width=2)),
        )
        fig.update_layout(title_text="Market share", title_font_size=14)
        apply_theme(fig, height=380)
        st.plotly_chart(fig, use_container_width=True, key="pie_mkt")

    st.markdown('<div class="sec">Volume distribution</div>', unsafe_allow_html=True)
    c3, c4 = st.columns(2)

    with c3:
        fig = px.bar(
            mkt_agg, x="market", y="volume",
            color="market", text_auto=".2s",
            labels={"volume": "Volume", "market": ""},
        )
        fig.update_traces(textposition="outside", textfont_size=11, marker_line_width=0)
        fig.update_layout(showlegend=False, title_text="Volume by Market", title_font_size=14)
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True, key="bar_vol")

    with c4:
        # Scatter: turnover vs volume per market
        fig = px.scatter(
            mkt_agg, x="volume", y="turnover",
            size="count", color="market",
            text="market",
            labels={"turnover": "Turnover (€)", "volume": "Volume", "count": "Securities"},
        )
        fig.update_traces(textposition="top center", textfont_size=10)
        fig.update_layout(title_text="Turnover vs Volume", title_font_size=14, showlegend=False)
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True, key="scatter_mkt")


# ── Tab 2: Sectors ──────────────────────────────────────────
with tab_sectors:
    st.markdown('<div class="sec">Sector activity</div>', unsafe_allow_html=True)

    # Filter out "Other" for cleaner view if many sectors exist
    sec_display = sec_agg[sec_agg["sector"] != "Other"].head(12)

    if not sec_display.empty:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                sec_display, x="turnover", y="sector",
                orientation="h", color="turnover",
                color_continuous_scale=["#1e3a5f", "#6ea8fe", "#c084fc"],
                labels={"turnover": "Turnover (€)", "sector": ""},
            )
            fig.update_layout(
                title_text="Turnover by Sector", title_font_size=14,
                yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False,
            )
            fig.update_traces(marker_line_width=0)
            apply_theme(fig, height=420)
            st.plotly_chart(fig, use_container_width=True, key="sec_bar")

        with c2:
            fig = px.pie(
                sec_display.head(8), values="turnover", names="sector",
                hole=0.55,
            )
            fig.update_traces(
                textposition="inside", textinfo="percent+label",
                textfont_size=10,
                marker=dict(line=dict(color="#0b0f19", width=2)),
            )
            fig.update_layout(title_text="Sector share", title_font_size=14)
            apply_theme(fig, height=420)
            st.plotly_chart(fig, use_container_width=True, key="sec_pie")

        # Sector table
        st.markdown('<div class="sec">Sector summary</div>', unsafe_allow_html=True)
        sec_table = sec_agg[["sector", "count", "turnover", "volume", "share", "avg_chg"]].copy()
        sec_table.columns = ["Sector", "Securities", "Turnover", "Volume", "Share %", "Avg Chg %"]
        sec_table["Turnover"] = sec_table["Turnover"].apply(fmt_eur)
        sec_table["Volume"] = sec_table["Volume"].apply(fmt_vol)
        sec_table["Share %"] = sec_table["Share %"].apply(lambda x: f"{x:.1f}%")
        sec_table["Avg Chg %"] = sec_table["Avg Chg %"].apply(fmt_pct)
        st.dataframe(sec_table, use_container_width=True, hide_index=True, height=440)
    else:
        st.markdown("""
        <div class="empty-state">
            <h3>Sector data is limited</h3>
            <p>ICB sector mapping covers major blue chips. Unmapped securities
            appear under "Other". Coverage improves as the mapping file grows.</p>
        </div>
        """, unsafe_allow_html=True)


# ── Tab 3: Tables ───────────────────────────────────────────
with tab_tables:
    st.markdown('<div class="sec">Top securities by turnover</div>', unsafe_allow_html=True)

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        mkt_options = ["All"] + sorted(df["market"].unique().tolist())
        mkt_filter = st.selectbox("Market", mkt_options, label_visibility="collapsed")
    with col_f2:
        sec_options = ["All"] + sorted(df["sector"].unique().tolist())
        sec_filter = st.selectbox("Sector", sec_options, label_visibility="collapsed")

    filtered = df.copy()
    if mkt_filter != "All":
        filtered = filtered[filtered["market"] == mkt_filter]
    if sec_filter != "All":
        filtered = filtered[filtered["sector"] == sec_filter]

    top_turnover = filtered.nlargest(60, "turnover")[
        ["name", "symbol", "market", "last_price", "change_pct", "volume", "turnover", "sector"]
    ].copy()
    top_turnover.columns = ["Name", "Symbol", "Market", "Last (€)", "Chg %", "Volume", "Turnover (€)", "Sector"]

    st.dataframe(
        top_turnover.style
            .format({"Last (€)": "{:.2f}", "Chg %": "{:+.2f}%", "Volume": "{:,.0f}", "Turnover (€)": "€{:,.0f}"})
            .map(
                lambda v: "color: #63e6be" if isinstance(v, (int, float)) and v > 0
                          else ("color: #ff8787" if isinstance(v, (int, float)) and v < 0 else ""),
                subset=["Chg %"]
            ),
        use_container_width=True,
        hide_index=True,
        height=540,
    )

    st.markdown('<div class="sec">Top movers</div>', unsafe_allow_html=True)

    # Top gainers and losers among liquid names
    liquid = filtered[filtered["turnover"] > filtered["turnover"].quantile(0.25)] if len(filtered) > 20 else filtered
    c1, c2 = st.columns(2)

    with c1:
        gainers = liquid.nlargest(15, "change_pct")[["name", "symbol", "market", "change_pct", "turnover"]].copy()
        gainers.columns = ["Name", "Symbol", "Market", "Chg %", "Turnover (€)"]
        if not gainers.empty:
            fig = px.bar(
                gainers.head(12), x="Symbol", y="Chg %",
                color="Chg %",
                color_continuous_scale=["#1e3a5f", "#63e6be"],
                text="Chg %",
            )
            fig.update_traces(texttemplate="%{text:+.1f}%", textposition="outside", textfont_size=10, marker_line_width=0)
            fig.update_layout(title_text="Top Gainers", title_font_size=14, coloraxis_showscale=False)
            apply_theme(fig, height=340)
            st.plotly_chart(fig, use_container_width=True, key="gainers")

    with c2:
        losers = liquid.nsmallest(15, "change_pct")[["name", "symbol", "market", "change_pct", "turnover"]].copy()
        losers.columns = ["Name", "Symbol", "Market", "Chg %", "Turnover (€)"]
        if not losers.empty:
            fig = px.bar(
                losers.head(12), x="Symbol", y="Chg %",
                color="Chg %",
                color_continuous_scale=["#ff8787", "#1e3a5f"],
                text="Chg %",
            )
            fig.update_traces(texttemplate="%{text:+.1f}%", textposition="outside", textfont_size=10, marker_line_width=0)
            fig.update_layout(title_text="Top Decliners", title_font_size=14, coloraxis_showscale=False)
            apply_theme(fig, height=340)
            st.plotly_chart(fig, use_container_width=True, key="losers")


# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown(
    '<div style="text-align:center;font-size:0.7rem;color:var(--c-text-3);padding:0.5rem 0 1rem;">'
    'Source: Euronext Live · Delayed ~15 min · Personal use only · Not for redistribution'
    '</div>',
    unsafe_allow_html=True,
)
