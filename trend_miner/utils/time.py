"""Time utilities for timezone-aware datetime handling."""

from datetime import datetime, timezone, timedelta
from typing import Optional
import pytz


def utcnow() -> datetime:
    """取得當前 UTC 時間 (tz-aware)"""
    return datetime.now(timezone.utc)


def to_utc(dt: datetime,tz_name: Optional[str] = None) -> datetime:
    """
    轉換時間為 UTC tz-aware datetime
    
    Args:
        dt: 輸入時間
        tz_name: 原時區名稱 (若 dt 為 naive)
    
    Returns:
        UTC tz-aware datetime
    """
    if dt.tzinfo is None:
        # Naive datetime，需要指定時區
        if tz_name:
            tz = pytz.timezone(tz_name)
            dt = tz.localize(dt)
        else:
            # 假設為 UTC
            dt = dt.replace(tzinfo=timezone.utc)
    
    # 轉換為 UTC
    return dt.astimezone(timezone.utc)


def calculate_lookback_date(days: int, tz_name: str = "UTC") -> datetime:
    """
    計算回溯日期
    
    Args:
        days: 回溯天數
        tz_name: 時區名稱
    
    Returns:
        UTC tz-aware datetime
    """
    now = utcnow()
    lookback = now - timedelta(days=days)
    return lookback


def format_iso8601(dt: datetime) -> str:
    """格式化為 ISO8601 字串"""
    return dt.isoformat()


def parse_iso8601(date_str: str) -> datetime:
    """解析 ISO8601 字串為 tz-aware datetime"""
    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return to_utc(dt)


def get_daily_bucket(dt: datetime) -> str:
    """
    取得日期桶 (YYYY-MM-DD)
    
    Args:
        dt: 時間 (會轉換為 UTC)
    
    Returns:
        YYYY-MM-DD 格式字串
    """
    utc_dt = to_utc(dt)
    return utc_dt.strftime("%Y-%m-%d")


def get_hourly_bucket(dt: datetime) -> str:
    """
    取得小時桶 (YYYY-MM-DDTHH:00:00Z)
    
    Args:
        dt: 時間 (會轉換為 UTC)
    
    Returns:
        ISO8601 格式字串
    """
    utc_dt = to_utc(dt)
    return utc_dt.replace(minute=0, second=0, microsecond=0).isoformat()
