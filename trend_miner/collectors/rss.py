"""
RSS Feed Collector

嚴格限制：僅使用 title/summary/link，不抓取全文。
"""

import feedparser
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging
from urllib.parse import urlparse

from trend_miner.models import ItemRecord
from trend_miner.config import RSSFeedConfig
from trend_miner.utils import time as time_utils
from trend_miner.utils import hashing
from trend_miner.processing.url_normalize import normalize_url, extract_domain

logger = logging.getLogger(__name__)


def parse_rss_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    解析 RSS 日期字串
    
    Args:
        date_str: RSS date string
    
    Returns:
        UTC tz-aware datetime or None
    """
    if not date_str:
        return None
    
    try:
        # feedparser 已經解析過了
        time_struct = feedparser._parse_date(date_str)
        if time_struct:
            dt = datetime(*time_struct[:6])
            return time_utils.to_utc(dt)
    except:
        pass
    
    return None


def infer_publisher(entry: Dict[str, Any], feed_title: str, canonical_url: str) -> str:
    """
    推斷 publisher name (顯示名稱，非 domain)
    
    優先序:
    1. entry.source.title
    2. entry.author
    3. feed.title
    
    Args:
        entry: RSS entry
        feed_title: Feed title
        canonical_url: Canonical URL
    
    Returns:
        Publisher name
    """
    # 1. Source title
    if hasattr(entry, 'source') and entry.source.get('title'):
        return entry.source['title']
    
    # 2. Author
    if entry.get('author'):
        return entry.author
    
    # 3. Feed title
    if feed_title:
        return feed_title
    
    # 4. Domain (fallback)
    parsed = urlparse(canonical_url)
    return parsed.netloc


def collect_from_feed(
    feed_config: RSSFeedConfig,
    run_id: str,
    max_items: int = 50,
    lookback_date: Optional[datetime] = None,
    tldextract_cache_dir: Optional[str] = None
) -> List[ItemRecord]:
    """
    從單一 RSS feed 收集資料
    
    Args:
        feed_config: Feed 設定
        run_id: 執行 ID
        max_items: 最多抓取數
        lookback_date: 回溯日期 (只取此日期之後的項目)
        tldextract_cache_dir: tldextract cache directory
    
    Returns:
        List of ItemRecord
    """
    items = []
    fetched_at = time_utils.utcnow()
    
    logger.info(f"Fetching RSS feed: {feed_config.name} ({feed_config.url})")
    
    try:
        # 解析 RSS feed
        feed = feedparser.parse(feed_config.url)
        
        if feed.bozo:
            logger.warning(f"Feed parsing warning for {feed_config.name}: {feed.bozo_exception}")
        
        feed_title = feed.feed.get('title', feed_config.name)
        entry_count = 0
        
        for entry in feed.entries[:max_items]:
            try:
                # 基本欄位
                title = entry.get('title', '').strip()
                summary = entry.get('summary', entry.get('description', '')).strip()
                link = entry.get('link', '').strip()
                
                if not title or not link:
                    continue
                
                # URL 正規化與 domain 提取
                canonical_url = normalize_url(link)
                publisher_domain = extract_domain(canonical_url, tldextract_cache_dir)
                
                # 發布時間
                published_str = entry.get('published', entry.get('updated', ''))
                published_at = parse_rss_date(published_str)
                
                if not published_at:
                    published_at = fetched_at
                
                # Lookback 過濾
                if lookback_date and published_at < lookback_date:
                    continue
                
                # Summary 處理
                has_summary = bool(summary)
                if not summary:
                    summary = title
                
                # Content hash & Item ID
                c_hash = hashing.content_hash(title, summary)
                item_id = hashing.url_hash(canonical_url)
                
                # Publisher name (顯示名稱)
                publisher_name = infer_publisher(entry, feed_title, canonical_url)
                
                # 建立 ItemRecord
                item = ItemRecord(
                    item_id=item_id,
                    run_id=run_id,
                    canonical_url=canonical_url,
                    publisher_domain=publisher_domain,
                    published_at=published_at,
                    fetched_at=fetched_at,
                    source_name=feed_config.name,
                    source_weight=feed_config.weight,
                    title=title,
                    summary=summary,
                    has_summary=has_summary,
                    text_len=len(title) + len(summary),
                    content_hash=c_hash,
                    topic_id=-1,  # 尚未分群
                    topic_signature="",  # 尚未生成
                    json_payload={
                        'publisher_name': publisher_name,
                        'category': feed_config.category,
                        'market': feed_config.market,
                        'language': feed_config.language or 'en',
                        'original_link': link
                    }
                )
                
                items.append(item)
                entry_count += 1
                
            except Exception as e:
                logger.error(f"Error processing entry from {feed_config.name}: {e}")
                continue
        
        logger.info(f"Collected {entry_count} items from {feed_config.name}")
        
    except Exception as e:
        logger.error(f"Error fetching feed {feed_config.name}: {e}")
    
    return items


def collect_all_feeds(
    feed_configs: List[RSSFeedConfig],
    run_id: str,
    max_items_per_feed: int = 50,
    lookback_days: int = 7,
    run_timezone: str = "UTC",
    tldextract_cache_dir: Optional[str] = None
) -> List[ItemRecord]:
    """
    從所有 feeds 收集資料
    
    Args:
        feed_configs: Feed 設定清單
        run_id: 執行 ID
        max_items_per_feed: 每個 feed 最多抓取數
        lookback_days: 回溯天數
        run_timezone: 執行時區
        tldextract_cache_dir: tldextract cache directory
    
    Returns:
        所有收集到的 ItemRecords
    """
    lookback_date = time_utils.calculate_lookback_date(lookback_days, run_timezone)
    all_items = []
    
    for feed_config in feed_configs:
        items = collect_from_feed(
            feed_config,
            run_id,
            max_items=max_items_per_feed,
            lookback_date=lookback_date,
            tldextract_cache_dir=tldextract_cache_dir
        )
        all_items.extend(items)
    
    logger.info(f"Total items collected: {len(all_items)}")
    return all_items
