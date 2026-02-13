"""
Narrative Signal Scoring (L1 Preliminary)

L1 的評分是初步的 signal，不是最終的 Gate Score。
"""

from typing import List
from datetime import datetime, timezone
import math
import logging

from trend_miner.models import TopicRecord
from trend_miner.utils.time import utcnow

logger = logging.getLogger(__name__)


def calculate_narrative_scores(
    topics: List[TopicRecord],
    watchlist_keywords: List[str] = None,
    watchlist_tickers: List[str] = None
) -> List[TopicRecord]:
    """
    計算每個 topic 的 narrative_signal_score (0-100)
    
    Args:
        topics: TopicRecords
        watchlist_keywords: 關注關鍵字
        watchlist_tickers: 關注股票代碼
    
    Returns:
        更新 narrative_signal_score 的 topics
    """
    watchlist_keywords = watchlist_keywords or []
    watchlist_tickers = watchlist_tickers or []
    
    now = utcnow()
    
    for topic in topics:
        score_breakdown = {}
        
        # 1. Volume Score (log scaling)
        volume_score = calculate_volume_score(topic.topic_volume)
        score_breakdown['volume_score'] = volume_score
        
        # 2. Velocity Score (recency)
        velocity_score = calculate_velocity_score(topic, now)
        score_breakdown['velocity_score'] = velocity_score
        
        # 3. Source Score (quality + diversity)
        source_score = calculate_source_score(topic)
        score_breakdown['source_score'] = source_score
        
        # 4. Watchlist Bonus
        watchlist_bonus = calculate_watchlist_bonus(
            topic,
            watchlist_keywords,
            watchlist_tickers
        )
        score_breakdown['watchlist_bonus'] = watchlist_bonus
        
        # Final score (weighted combination)
        final_score = (
            volume_score * 0.25 +
            velocity_score * 0.35 +
            source_score * 0.30 +
            watchlist_bonus * 0.10
        )
        
        # Clamp to 0-100
        final_score = max(0, min(100, final_score))
        
        topic.narrative_signal_score = round(final_score, 2)
        topic.json_payload['score_breakdown'] = score_breakdown
        
        logger.debug(f"Topic {topic.topic_id} score: {final_score:.2f} " +
                    f"(vol={volume_score:.1f}, vel={velocity_score:.1f}, " +
                    f"src={source_score:.1f}, wl={watchlist_bonus:.1f})")
    
    return topics


def calculate_volume_score(volume: int) -> float:
    """
    Volume score (log scaling)
    
    Args:
        volume: 文章數
    
    Returns:
        Score 0-100
    """
    if volume <= 0:
        return 0.0
    
    # Log scaling with base adjustment
    # 10 articles -> ~50, 50 articles -> ~75, 200 articles -> ~90
    score = 20 * math.log10(volume + 1) + 20
    return max(0, min(100, score))


def calculate_velocity_score(topic: TopicRecord, now: datetime) -> float:
    """
    Velocity score (近期性加權)
    
    基於最新文章的時間與 now 的距離，越新越高分。
    
    Args:
        topic: TopicRecord
        now: 當前時間
    
    Returns:
        Score 0-100
    """
    if not topic.last_seen_at:
        return 0.0
    
    # 計算最新文章距離現在的小時數
    delta = now - topic.last_seen_at
    hours_ago = delta.total_seconds() / 3600
    
    # 衰減函數：1 小時內 = 100, 24 小時 = 50, 7 天 = ~10
    # score = 100 * exp(-hours_ago / 24)
    score = 100 * math.exp(-hours_ago / 24)
    
    return max(0, min(100, score))


def calculate_source_score(topic: TopicRecord) -> float:
    """
    Source score (品質 + 多樣性)
    
    Args:
        topic: TopicRecord
    
    Returns:
        Score 0-100
    """
    # 平均來源權重 (0-1) -> 0-60
    weight_score = topic.avg_source_weight * 60
    
    # 多樣性：unique domains (log scaling) -> 0-40
    # 5 domains -> ~30, 10 domains -> ~40
    if topic.unique_domains > 1:
        diversity_score = 15 * math.log10(topic.unique_domains) + 15
    else:
        diversity_score = 0
    
    diversity_score = max(0, min(40, diversity_score))
    
    score = weight_score + diversity_score
    return max(0, min(100, score))


def calculate_watchlist_bonus(
    topic: TopicRecord,
    watchlist_keywords: List[str],
    watchlist_tickers: List[str]
) -> float:
    """
    Watchlist bonus
    
    檢查 topic keywords 或 representative items 是否命中 watchlist
    
    Args:
        topic: TopicRecord
        watchlist_keywords: 關注關鍵字
        watchlist_tickers: 關注股票代碼
    
    Returns:
        Bonus score 0-100
    """
    if not watchlist_keywords and not watchlist_tickers:
        return 0.0
    
    # 合併 watchlist (lowercase)
    watchlist_terms = set([kw.lower() for kw in watchlist_keywords + watchlist_tickers])
    
    # 檢查 keywords
    topic_keywords_lower = [kw.lower() for kw in topic.top_keywords]
    keyword_hits = sum(1 for kw in topic_keywords_lower if kw in watchlist_terms)
    
    # 檢查 representative items (title)
    rep_title_hits = 0
    for item in topic.representative_items:
        title_lower = item.title.lower()
        if any(term in title_lower for term in watchlist_terms):
            rep_title_hits += 1
    
    # 計算 bonus
    if keyword_hits > 0 or rep_title_hits > 0:
        # 命中 watchlist，給予固定 bonus
        return 80.0
    
    return 0.0
