import streamlit as st
import pandas as pd
from modules.ticker_universe import get_universe
from modules.price_collector import collect_prices
from modules.type_classifier import classify_stock
from modules.market_regime import get_market_regime

st.set_page_config(page_title='US Momentum Surge Scanner', layout='wide')

st.title('US Momentum Surge Scanner - Phase 1 MVP')

regime = get_market_regime()
st.info(f"Market Regime: {regime['regime']}")

universe = get_universe()
raw = collect_prices(universe[:10])

results = []
for stock in raw:
    classified = classify_stock(stock)
    results.append({
        'Ticker': stock.get('ticker'),
        'Type': classified.get('primary_type'),
        'Score': classified.get('score'),
        'Confidence': classified.get('confidence'),
        'Regime': regime['regime'],
        'Kill-Switch': 'PASS',
        'Data Quality': stock.get('data_quality', 'APPROXIMATE')
    })

if len(results) == 0:
    st.warning('오늘은 강한 후보 없음')
else:
    st.dataframe(pd.DataFrame(results), use_container_width=True)
