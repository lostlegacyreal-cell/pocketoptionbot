"""
signal_engine.py
Signal generation logic: fetch market data (via yfinance fallback), compute indicators
(RSI14, EMA50), detect simple support/resistance, candlestick pattern checks,
volume spike filter, then produce a CALL/PUT signal with confidence score.

NOTE: Uses yfinance for OHLCV data via synchronous calls executed in thread
executor to avoid blocking the async bot.
"""
import asyncio
from datetime import datetime, timedelta
from typing import Tuple, Optional
import numpy as np
import pandas as pd
import config
import logging

logger = logging.getLogger(__name__)

# yfinance is optional; used for public market data retrieval
try:
    import yfinance as yf
    HAS_YFINANCE = True
except Exception:
    HAS_YFINANCE = False


# ------------------- Technical indicators (pure pandas) -------------------

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.rolling(window=length, min_periods=1).mean()
    ma_down = down.rolling(window=length, min_periods=1).mean()
    rs = ma_up / (ma_down + 1e-8)
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val


def volume_spike(volume: pd.Series, window: int = 20, threshold: float = 2.0) -> pd.Series:
    sma = volume.rolling(window=window, min_periods=1).mean()
    return volume / (sma + 1e-8) >= threshold


def detect_support_resistance(close: pd.Series, window: int = 10) -> Tuple[float, float]:
    # Simple pivot-based support/resistance
    highs = close.rolling(window=window, center=True).max()
    lows = close.rolling(window=window, center=True).min()
    recent_high = highs.dropna().iloc[-1] if not highs.dropna().empty else close.max()
    recent_low = lows.dropna().iloc[-1] if not lows.dropna().empty else close.min()
    return float(recent_low), float(recent_high)


def bullish_engulfing(df: pd.DataFrame) -> bool:
    # Simple bullish engulfing pattern on last two candles
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    return (last['close'] > last['open']) and (prev['close'] < prev['open']) and (last['close'] > prev['open']) and (last['open'] < prev['close'])


def bearish_engulfing(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    return (last['close'] < last['open']) and (prev['close'] > prev['open']) and (last['open'] > prev['close']) and (last['close'] < prev['open'])


# ------------------- Data fetching -------------------

async def fetch_ohlcv_yfinance(symbol: str, period_minutes: int, count: int = 200) -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV with yfinance by mapping symbol to a ticker.
    For FX pairs like EURUSD we use major proxies (e.g., EURUSD=X)
    Returns DataFrame with columns: ['open','high','low','close','volume'] and datetime index.
    """
    if not HAS_YFINANCE:
        logger.warning("yfinance not available")
        return None

    # Map common FX pairs to yfinance tickers
    ticker = symbol
    if symbol.upper().endswith('USD') and len(symbol) == 6:
        # e.g., EURUSD -> EURUSD=X
        ticker = f"{symbol[:3]}{symbol[3:]}=X"
    # else for stocks or BTCUSD, yfinance typically uses e.g. BTC-USD
    if symbol.upper() == 'BTCUSD':
        ticker = 'BTC-USD'

    interval = '1m' if period_minutes == 1 else ('5m' if period_minutes == 5 else '15m')
    period = '1d' if period_minutes <= 15 else '5d'

    loop = asyncio.get_event_loop()
    try:
        df = await loop.run_in_executor(None, lambda: yf.download(tickers=ticker, period=period, interval=interval, progress=False))
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'
        })
        df = df[['open', 'high', 'low', 'close', 'volume']].tail(count)
        df = df.reset_index()
        return df
    except Exception as e:
        logger.exception(f"yfinance fetch error: {e}")
        return None


# ------------------- Signal generation -------------------

async def generate_signal(symbol: str, timeframe_min: int) -> Optional[dict]:
    """
    Main entry to generate a signal for given symbol and timeframe in minutes.
    Returns dict with keys: asset, timeframe, direction, confidence, entry_time, expiry_time
    """
    # Fetch data
    df = None
    if HAS_YFINANCE:
        df = await fetch_ohlcv_yfinance(symbol, timeframe_min, count=200)
    if df is None or df.empty:
        logger.info("No OHLCV data available for symbol; signal engine requires manual input or yfinance")
        return None

    # Ensure numeric
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)

    # Resample if needed: yfinance already returns timeframe bars
    close = df['close']

    # Compute indicators
    df['ema50'] = ema(df['close'], span=50)
    df['rsi14'] = rsi(df['close'], length=14)
    df['vol_spike'] = volume_spike(df['volume'], window=20, threshold=2.0)

    # Support/resistance
    s, r = detect_support_resistance(df['close'], window=10)

    # Candlestick confirmation
    is_bull = bullish_engulfing(df)
    is_bear = bearish_engulfing(df)

    # Basic rules for signal
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else latest

    score_up = 0.0
    score_down = 0.0

    # EMA trend
    if latest['close'] > latest['ema50']:
        score_up += 0.3
    else:
        score_down += 0.3

    # RSI filter (oversold/overbought)
    rsi_val = latest['rsi14']
    if rsi_val < 35:
        score_up += 0.25
    elif rsi_val > 65:
        score_down += 0.25

    # Candlestick
    if is_bull:
        score_up += 0.2
    if is_bear:
        score_down += 0.2

    # Volume spike supports direction
    if latest['vol_spike']:
        # If volume spike and price moved up in that candle
        if latest['close'] > latest['open']:
            score_up += 0.15
        else:
            score_down += 0.15

    # Support/Resistance proximity
    # If close near support -> bias up; near resistance -> bias down
    try:
        if abs(latest['close'] - s) / s < 0.0015:
            score_up += 0.1
        if abs(latest['close'] - r) / r < 0.0015:
            score_down += 0.1
    except Exception:
        pass

    # Combine into probability-like score
    prob_up = min(max(score_up, 0.0), 1.0)
    prob_down = min(max(score_down, 0.0), 1.0)

    # Confidence normalized
    confidence = prob_up if prob_up > prob_down else prob_down

    direction = 'CALL' if prob_up > prob_down else 'PUT'

    # Timestamping
    entry_time = datetime.utcnow().isoformat() + 'Z'
    expiry = datetime.utcnow() + timedelta(minutes=timeframe_min)
    expiry_time = expiry.isoformat() + 'Z'

    # Basic confidence percentage display
    conf_pct = round(confidence * 100, 1)

    signal = {
        'asset': symbol,
        'timeframe': f"{timeframe_min}M",
        'direction': direction,
        'confidence': conf_pct,
        'entry_time': entry_time,
        'expiry_time': expiry_time,
        'metadata': {
            'rsi': float(rsi_val),
            'ema50': float(latest['ema50']),
            'support': s,
            'resistance': r,
            'vol_spike': bool(latest['vol_spike'])
        }
    }

    return signal


# ------------------- Manual input helper -------------------
async def generate_signal_from_dataframe(df: pd.DataFrame, timeframe_min: int) -> Optional[dict]:
    # Accepts pre-prepared OHLCV dataframe and runs same logic
    if df is None or df.empty:
        return None
    # Align columns
    df = df.copy()
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    df['ema50'] = ema(df['close'], span=50)
    df['rsi14'] = rsi(df['close'], length=14)
    df['vol_spike'] = volume_spike(df['volume'], window=20, threshold=2.0)
    s, r = detect_support_resistance(df['close'], window=10)
    is_bull = bullish_engulfing(df)
    is_bear = bearish_engulfing(df)
    latest = df.iloc[-1]

    # reuse same scoring logic
    score_up = 0.0
    score_down = 0.0
    if latest['close'] > latest['ema50']:
        score_up += 0.3
    else:
        score_down += 0.3
    rsi_val = latest['rsi14']
    if rsi_val < 35:
        score_up += 0.25
    elif rsi_val > 65:
        score_down += 0.25
    if is_bull:
        score_up += 0.2
    if is_bear:
        score_down += 0.2
    if latest['vol_spike']:
        if latest['close'] > latest['open']:
            score_up += 0.15
        else:
            score_down += 0.15
    try:
        if abs(latest['close'] - s) / s < 0.0015:
            score_up += 0.1
        if abs(latest['close'] - r) / r < 0.0015:
            score_down += 0.1
    except Exception:
        pass
    prob_up = min(max(score_up, 0.0), 1.0)
    prob_down = min(max(score_down, 0.0), 1.0)
    confidence = prob_up if prob_up > prob_down else prob_down
    direction = 'CALL' if prob_up > prob_down else 'PUT'
    entry_time = datetime.utcnow().isoformat() + 'Z'
    expiry = datetime.utcnow() + timedelta(minutes=timeframe_min)
    expiry_time = expiry.isoformat() + 'Z'
    conf_pct = round(confidence * 100, 1)
    return {
        'asset': 'MANUAL',
        'timeframe': f"{timeframe_min}M",
        'direction': direction,
        'confidence': conf_pct,
        'entry_time': entry_time,
        'expiry_time': expiry_time,
        'metadata': {
            'rsi': float(rsi_val),
            'ema50': float(latest['ema50']),
            'support': s,
            'resistance': r,
            'vol_spike': bool(latest['vol_spike'])
        }
    }
