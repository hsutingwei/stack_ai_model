"""
Topic Aggregation

將 item-level 數據聚合成 topic-level TopicRecord，產生穩定的 topic_signature。
"""

from typing import List, Dict
from collections import Counter, defaultdict
from datetime import datetime
import logging

from trend_miner.models import ItemRecord, TopicRecord, TopicBucket, RepresentativeItem
from trend_miner.utils import hashing
from trend_miner.utils.time import get_daily_bucket, get_hourly_bucket

logger = logging.getLogger(__name__)


def aggregate_topics(
    items: List[ItemRecord],
    topic_keywords: Dict[int, List[str]],
    bucket_interval: str = "daily"
) -> List[TopicRecord]:
    """
    將 items 聚合成 TopicRecords
    
    Args:
        items: 已分群的 ItemRecords
        topic_keywords: {topic_id: [keywords]} from BERTopic
        bucket_interval: Time bucket interval (daily | hourly)
    
    Returns:
        List of TopicRecord
    """
    # Group items by topic_id
    topic_groups: Dict[int, List[ItemRecord]] = defaultdict(list)
    
    for item in items:
        if item.topic_id != -1:  # Skip noise
            topic_groups[item.topic_id].append(item)
    
    topic_records = []
    
    for topic_id, topic_items in topic_groups.items():
        topic_record = create_topic_record(
            topic_id,
            topic_items,
            topic_keywords.get(topic_id, []),
            bucket_interval
        )
        topic_records.append(topic_record)
    
    logger.info(f"Created {len(topic_records)} TopicRecords")
    return topic_records


def create_topic_record(
    topic_id: int,
    items: List[ItemRecord],
    keywords: List[str],
    bucket_interval: str
) -> TopicRecord:
    """
    建立單一 TopicRecord
    
    Args:
        topic_id: BERTopic ID
        items: 該 topic 的所有 items
        keywords: Topic keywords
        bucket_interval: Time bucket interval
    
    Returns:
        TopicRecord
    """
    if not items:
        raise ValueError("Cannot create TopicRecord with empty items")
    
    run_id = items[0].run_id
    
    # 統計指標
    topic_volume = len(items)
    
    # Unique domains
    domains = [item.publisher_domain for item in items]
    domain_counts = Counter(domains)
    unique_domains = len(domain_counts)
    top_domains = [domain for domain, count in domain_counts.most_common(5)]
    
    # Average source weight
    avg_source_weight = sum(item.source_weight for item in items) / topic_volume
    
    # Duplicate ratio (based on content_hash)
    content_hashes = [item.content_hash for item in items]
    unique_hashes = len(set(content_hashes))
    duplicate_ratio = 1.0 - (unique_hashes / topic_volume)
    
    # Time range
    published_dates = [item.published_at for item in items]
    first_seen_at = min(published_dates)
    last_seen_at = max(published_dates)
    
    # Time bucketing
    counts_by_bucket = calculate_time_buckets(items, bucket_interval)
    
    # Representative items (選取策略：最新 + 高權重來源)
    representative_items = select_representative_items(items, n=5)
    
    # Topic signature (穩定 hash)
    topic_sig = hashing.topic_signature(keywords[:10], top_domains[:5])
    
    # 更新所有 items 的 topic_signature
    for item in items:
        item.topic_signature = topic_sig
    
    # 建立 TopicRecord
    topic_record = TopicRecord(
        run_id=run_id,
        topic_id=topic_id,
        topic_signature=topic_sig,
        top_keywords=keywords[:15],
        topic_volume=topic_volume,
        unique_domains=unique_domains,
        avg_source_weight=avg_source_weight,
        duplicate_ratio=duplicate_ratio,
        counts_by_bucket=counts_by_bucket,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        representative_items=representative_items,
        narrative_signal_score=None,  # 後續計算
        json_payload={
            'top_domains': top_domains,
            'domain_counts': dict(domain_counts.most_common(10))
        }
    )
    
    return topic_record


def calculate_time_buckets(
    items: List[ItemRecord],
    bucket_interval: str
) -> List[TopicBucket]:
    """
    計算時間桶統計
    
    Args:
        items: ItemRecords
        bucket_interval: daily | hourly
    
    Returns:
        List of TopicBucket (sorted by bucket_start)
    """
    bucket_counts: Dict[str, int] = defaultdict(int)
    
    for item in items:
        if bucket_interval == "daily":
            bucket_key = get_daily_bucket(item.published_at)
        elif bucket_interval == "hourly":
            bucket_key = get_hourly_bucket(item.published_at)
        else:
            raise ValueError(f"Unsupported bucket_interval: {bucket_interval}")
        
        bucket_counts[bucket_key] += 1
    
    # 轉換成 TopicBucket 並排序
    buckets = [
        TopicBucket(bucket_start=bucket_key, count=count)
        for bucket_key, count in bucket_counts.items()
    ]
    
    buckets.sort(key=lambda b: b.bucket_start)
    return buckets


def select_representative_items(
    items: List[ItemRecord],
    n: int = 5
) -> List[RepresentativeItem]:
    """
    選取代表性文章
    
    策略：
    1. 先按 source_weight 降序 + published_at 降序排序
    2. 取前 n 篇
    
    Args:
        items: ItemRecords
        n: 數量
    
    Returns:
        List of RepresentativeItem
    """
    # 排序：權重高的優先，時間新的優先
    sorted_items = sorted(
        items,
        key=lambda x: (-x.source_weight, -x.published_at.timestamp())
    )
    
    selected = sorted_items[:n]
    
    representatives = [
        RepresentativeItem(
            url=item.canonical_url,
            title=item.title,
            domain=item.publisher_domain,
            published_at=item.published_at,
            summary=item.summary[:200]  # 限制長度
        )
        for item in selected
    ]
    
    return representatives
