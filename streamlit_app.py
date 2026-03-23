"""
Euronext Market Activity Dashboard
Yahoo Finance proxy edition — broad-universe market activity approximation.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import yfinance as yf
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

MARKETS = {
    "Paris":     {"flag": "🇫🇷"},
    "Amsterdam": {"flag": "🇳🇱"},
    "Brussels":  {"flag": "🇧🇪"},
    "Lisbon":    {"flag": "🇵🇹"},
    "Milan":     {"flag": "🇮🇹"},
    "Oslo":      {"flag": "🇳🇴"},
    "Dublin":    {"flag": "🇮🇪"},
}

# Broad-ish starter universe by market.
# Expand this list over time.
UNIVERSE = [
    # Paris
    {"ticker": "MC.PA", "name": "LVMH", "market": "Paris", "sector": "Consumer Discretionary"},
    {"ticker": "OR.PA", "name": "L'Oreal", "market": "Paris", "sector": "Consumer Staples"},
    {"ticker": "TTE.PA", "name": "TotalEnergies", "market": "Paris", "sector": "Energy"},
    {"ticker": "SAN.PA", "name": "Sanofi", "market": "Paris", "sector": "Health Care"},
    {"ticker": "AIR.PA", "name": "Airbus", "market": "Paris", "sector": "Industrials"},
    {"ticker": "SU.PA", "name": "Schneider Electric", "market": "Paris", "sector": "Industrials"},
    {"ticker": "BNP.PA", "name": "BNP Paribas", "market": "Paris", "sector": "Financials"},
    {"ticker": "ACA.PA", "name": "Credit Agricole", "market": "Paris", "sector": "Financials"},
    {"ticker": "GLE.PA", "name": "Societe Generale", "market": "Paris", "sector": "Financials"},
    {"ticker": "ENGI.PA", "name": "Engie", "market": "Paris", "sector": "Utilities"},
    {"ticker": "CAP.PA", "name": "Capgemini", "market": "Paris", "sector": "Technology"},
    {"ticker": "KER.PA", "name": "Kering", "market": "Paris", "sector": "Consumer Discretionary"},
    {"ticker": "VIE.PA", "name": "Veolia", "market": "Paris", "sector": "Utilities"},
    {"ticker": "DG.PA", "name": "Vinci", "market": "Paris", "sector": "Industrials"},
    {"ticker": "RI.PA", "name": "Pernod Ricard", "market": "Paris", "sector": "Consumer Staples"},

    # Amsterdam
    {"ticker": "ASML.AS", "name": "ASML", "market": "Amsterdam", "sector": "Technology"},
    {"ticker": "INGA.AS", "name": "ING", "market": "Amsterdam", "sector": "Financials"},
    {"ticker": "AD.AS", "name": "Ahold Delhaize", "market": "Amsterdam", "sector": "Consumer Staples"},
    {"ticker": "PHIA.AS", "name": "Philips", "market": "Amsterdam", "sector": "Health Care"},
    {"ticker": "WKL.AS", "name": "Wolters Kluwer", "market": "Amsterdam", "sector": "Industrials"},
    {"ticker": "ADYEN.AS", "name": "Adyen", "market": "Amsterdam", "sector": "Technology"},
    {"ticker": "PRX.AS", "name": "Prosus", "market": "Amsterdam", "sector": "Technology"},
    {"ticker": "AKZA.AS", "name": "Akzo Nobel", "market": "Amsterdam", "sector": "Basic Materials"},
    {"ticker": "RAND.AS", "name": "Randstad", "market": "Amsterdam", "sector": "Industrials"},
    {"ticker": "MT.AS", "name": "ArcelorMittal", "market": "Amsterdam", "sector": "Basic Materials"},

    # Brussels
    {"ticker": "ABI.BR", "name": "AB InBev", "market": "Brussels", "sector": "Consumer Staples"},
    {"ticker": "UCB.BR", "name": "UCB", "market": "Brussels", "sector": "Health Care"},
    {"ticker": "KBC.BR", "name": "KBC", "market": "Brussels", "sector": "Financials"},
    {"ticker": "SOLB.BR", "name": "Solvay", "market": "Brussels", "sector": "Basic Materials"},
    {"ticker": "UMI.BR", "name": "Umicore", "market": "Brussels", "sector": "Basic Materials"},
    {"ticker": "AZE.BR", "name": "Azelis", "market": "Brussels", "sector": "Industrials"},
    {"ticker": "COFB.BR", "name": "Cofinimmo", "market": "Brussels", "sector": "Real Estate"},

    # Lisbon
    {"ticker": "EDP.LS", "name": "EDP", "market": "Lisbon", "sector": "Utilities"},
    {"ticker": "GALP.LS", "name": "Galp", "market": "Lisbon", "sector": "Energy"},
    {"ticker": "JMT.LS", "name": "Jerónimo Martins", "market": "Lisbon", "sector": "Consumer Staples"},
    {"ticker": "BCP.LS", "name": "BCP", "market": "Lisbon", "sector": "Financials"},
    {"ticker": "RENE.LS", "name": "REN", "market": "Lisbon", "sector": "Utilities"},
    {"ticker": "SON.LS", "name": "Sonae", "market": "Lisbon", "sector": "Consumer Staples"},

    # Milan
    {"ticker": "ENI.MI", "name": "Eni", "market": "Milan", "sector": "Energy"},
    {"ticker": "ISP.MI", "name": "Intesa Sanpaolo", "market": "Milan", "sector": "Financials"},
    {"ticker": "UCG.MI", "name": "UniCredit", "market": "Milan", "sector": "Financials"},
    {"ticker": "ENEL.MI", "name": "Enel", "market": "Milan", "sector": "Utilities"},
    {"ticker": "STM.MI", "name": "STMicroelectronics", "market": "Milan", "sector": "Technology"},
    {"ticker": "TIT.MI", "name": "Telecom Italia", "market": "Milan", "sector": "Telecommunications"},
    {"ticker": "MONC.MI", "name": "Moncler", "market": "Milan", "sector": "Consumer Discretionary"},
    {"ticker": "G.MI", "name": "Generali", "market": "Milan", "sector": "Financials"},

    # Oslo
    {"ticker": "EQNR.OL", "name": "Equinor", "market": "Oslo", "sector": "Energy"},
    {"ticker": "TEL.OL", "name": "Telenor", "market": "Oslo", "sector": "Telecommunications"},
    {"ticker": "DNB.OL", "name": "DNB Bank", "market": "Oslo", "sector": "Financials"},
    {"ticker": "MOWI.OL", "name": "Mowi", "market": "Oslo", "sector": "Consumer Staples"},
    {"ticker": "ORK.OL", "name": "Orkla", "market": "Oslo", "sector": "Consumer Staples"},
    {"ticker": "YAR.OL", "name": "Yara", "market": "Oslo", "sector": "Basic Materials"},
    {"ticker": "NHY.OL", "name": "Norsk Hydro", "market": "Oslo", "sector": "Basic Materials"},

    # Dublin
    {"ticker": "CRH.L", "name": "CRH", "market": "Dublin", "sector": "Basic Materials"},
    {"ticker": "RYAAY", "name": "Ryanair ADR", "market": "Dublin", "sector": "Industrials"},
    {"ticker": "A5G.IR", "name": "AIB Group", "market": "Dublin", "sector": "Financials"},
    {"ticker": "KRX.IR", "name": "Kerry Group", "market": "Dublin", "sector": "Consumer Staples"},
    {"ticker": "GL9.IR", "name": "Glanbia", "market": "Dublin", "sector": "Consumer Staples"},
]

REQUEST_TIMEOUT = 15


# ═══════════════════════════════════════════════════════════════
# HELPERS
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

def ping_internet() -> bool:
    try:
        requests.get("https://query1.finance.yahoo.com", timeout=REQUEST_TIMEOUT)
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════

@st.cache_data(ttl=600, show_spinner=False)
def fetch_all_data():
    universe_df = pd.DataFrame(UNIVERSE)
    tickers = universe_df["ticker"].tolist()

    try:
        data = yf.download(
            tickers=tickers,
            period="1d",
            interval="1m",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False,
        )
    except Exception:
        return pd.DataFrame()

    rows = []

    for item in UNIVERSE:
        ticker = item["ticker"]
        try:
            if ticker not in data.columns.get_level_values(0):
                continue

            tdf = data[ticker].copy()
            tdf = tdf.dropna(how="all")
            if tdf.empty:
                continue

            last_price = float(tdf["Close"].dropna().iloc[-1]) if "Close" in tdf else 0.0
            prev_close = float(tdf["Close"].dropna().iloc[0]) if "Close" in tdf else 0.0
            volume = float(tdf["Volume"].fillna(0).sum()) if "Volume" in tdf else 0.0

            if last_price <= 0 and prev_close <= 0:
                continue

            change_pct = ((last_price / prev_close) - 1) * 100 if prev_close > 0 else 0.0
            turnover = last_price * volume

            rows.append({
                "ticker": ticker,
                "name": item["name"],
                "market": item["market"],
                "sector": item["sector"],
                "last_price": last_price,
                "change_pct": change_pct,
                "volume": volume,
                "turnover": turnover,
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
# PAGE SETUP
# ═══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Euronext Activity Monitor",
    page_icon="◆",
    layout="wide",
)

st.title("◆ Euronext Activity Monitor")
st.caption("Yahoo Finance proxy · broad-universe approximation of activity by Euronext market")

if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
    st.session_state.last_fetch = None

col_a, col_b = st.columns([5, 1])
with col_b:
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.session_state.df = fetch_all_data()
        st.session_state.last_fetch = datetime.now(timezone.utc)

if st.session_state.df.empty:
    with st.spinner("Loading market data…"):
        st.session_state.df = fetch_all_data()
        st.session_state.last_fetch = datetime.now(timezone.utc)

df = st.session_state.df

if st.session_state.last_fetch:
    st.caption(f"Last refresh: {st.session_state.last_fetch.strftime('%H:%M:%S UTC · %d %b %Y')}")

if df.empty:
    if not ping_internet():
        st.error("No data available. The app could not reach Yahoo Finance.")
    else:
        st.error("No data available right now from Yahoo Finance for the current ticker universe.")
    st.stop()


# ═══════════════════════════════════════════════════════════════
# AGGREGATES
# ═══════════════════════════════════════════════════════════════

total_turnover = df["turnover"].sum()
total_volume = df["volume"].sum()
total_instruments = len(df)
markets_active = df["market"].nunique()

df["weight"] = df["turnover"] / total_turnover if total_turnover > 0 else 0
weighted_chg = (df["change_pct"] * df["weight"]).sum()

market_agg = (
    df.groupby("market", as_index=False)
      .agg(
          turnover=("turnover", "sum"),
          volume=("volume", "sum"),
          count=("ticker", "count"),
          avg_chg=("change_pct", "mean"),
      )
      .sort_values("turnover", ascending=False)
)
market_agg["share"] = market_agg["turnover"] / total_turnover * 100 if total_turnover > 0 else 0

sector_agg = (
    df.groupby("sector", as_index=False)
      .agg(
          turnover=("turnover", "sum"),
          volume=("volume", "sum"),
          count=("ticker", "count"),
          avg_chg=("change_pct", "mean"),
      )
      .sort_values("turnover", ascending=False)
)
sector_agg["share"] = sector_agg["turnover"] / total_turnover * 100 if total_turnover > 0 else 0


# ═══════════════════════════════════════════════════════════════
# KPI ROW
# ═══════════════════════════════════════════════════════════════

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Turnover", fmt_eur(total_turnover))
c2.metric("Total Volume", fmt_vol(total_volume))
c3.metric("Tracked Securities", f"{total_instruments:,}")
c4.metric("Markets Reporting", f"{markets_active} / {len(MARKETS)}")

delta_text = f"Weighted avg change: {fmt_pct(weighted_chg)}"
st.caption(delta_text)


# ═══════════════════════════════════════════════════════════════
# MARKET BREAKDOWN
# ═══════════════════════════════════════════════════════════════

st.subheader("Market breakdown")

display_market = market_agg.copy()
display_market["flag"] = display_market["market"].map(lambda x: MARKETS.get(x, {}).get("flag", "🏛️"))
display_market["market"] = display_market["flag"] + " " + display_market["market"]
display_market["turnover_fmt"] = display_market["turnover"].map(fmt_eur)
display_market["volume_fmt"] = display_market["volume"].map(fmt_vol)
display_market["share_fmt"] = display_market["share"].map(lambda x: f"{x:.1f}%")
display_market["avg_chg_fmt"] = display_market["avg_chg"].map(fmt_pct)

st.dataframe(
    display_market[["market", "count", "turnover_fmt", "volume_fmt", "share_fmt", "avg_chg_fmt"]]
        .rename(columns={
            "market": "Market",
            "count": "Securities",
            "turnover_fmt": "Turnover",
            "volume_fmt": "Volume",
            "share_fmt": "Share",
            "avg_chg_fmt": "Avg Chg",
        }),
    use_container_width=True,
    hide_index=True,
)

fig_market = px.bar(
    market_agg,
    x="market",
    y="turnover",
    color="market",
    text_auto=".2s",
    title="Turnover by Market",
)
fig_market.update_layout(showlegend=False)
st.plotly_chart(fig_market, use_container_width=True)

fig_market_share = px.pie(
    market_agg,
    values="turnover",
    names="market",
    hole=0.5,
    title="Market Share of Total Turnover",
)
st.plotly_chart(fig_market_share, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# SECTOR BREAKDOWN
# ═══════════════════════════════════════════════════════════════

st.subheader("Sector breakdown")

display_sector = sector_agg.copy()
display_sector["turnover_fmt"] = display_sector["turnover"].map(fmt_eur)
display_sector["volume_fmt"] = display_sector["volume"].map(fmt_vol)
display_sector["share_fmt"] = display_sector["share"].map(lambda x: f"{x:.1f}%")
display_sector["avg_chg_fmt"] = display_sector["avg_chg"].map(fmt_pct)

st.dataframe(
    display_sector[["sector", "count", "turnover_fmt", "volume_fmt", "share_fmt", "avg_chg_fmt"]]
        .rename(columns={
            "sector": "Sector",
            "count": "Securities",
            "turnover_fmt": "Turnover",
            "volume_fmt": "Volume",
            "share_fmt": "Share",
            "avg_chg_fmt": "Avg Chg",
        }),
    use_container_width=True,
    hide_index=True,
)

fig_sector = px.bar(
    sector_agg.head(12),
    x="turnover",
    y="sector",
    orientation="h",
    color="turnover",
    title="Top Sectors by Turnover",
)
fig_sector.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_sector, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# TOP SECURITIES
# ═══════════════════════════════════════════════════════════════

st.subheader("Top securities")

market_filter = st.selectbox("Filter by market", ["All"] + sorted(df["market"].unique().tolist()))
sector_filter = st.selectbox("Filter by sector", ["All"] + sorted(df["sector"].unique().tolist()))

filtered = df.copy()
if market_filter != "All":
    filtered = filtered[filtered["market"] == market_filter]
if sector_filter != "All":
    filtered = filtered[filtered["sector"] == sector_filter]

top_turnover = filtered.sort_values("turnover", ascending=False).copy()
top_turnover["turnover_fmt"] = top_turnover["turnover"].map(fmt_eur)
top_turnover["volume_fmt"] = top_turnover["volume"].map(fmt_vol)
top_turnover["change_fmt"] = top_turnover["change_pct"].map(fmt_pct)
top_turnover["last_price_fmt"] = top_turnover["last_price"].map(lambda x: f"{x:,.2f}")

st.dataframe(
    top_turnover[["name", "ticker", "market", "sector", "last_price_fmt", "change_fmt", "volume_fmt", "turnover_fmt"]]
        .rename(columns={
            "name": "Name",
            "ticker": "Ticker",
            "market": "Market",
            "sector": "Sector",
            "last_price_fmt": "Last",
            "change_fmt": "Chg %",
            "volume_fmt": "Volume",
            "turnover_fmt": "Turnover",
        }),
    use_container_width=True,
    hide_index=True,
    height=500,
)

st.markdown("---")
st.caption(
    "Source: Yahoo Finance via yfinance · This is a broad-universe proxy, not an official Euronext market-total feed."
)

