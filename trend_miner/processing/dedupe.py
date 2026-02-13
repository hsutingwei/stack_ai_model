"""
Deduplication logic

去重策略: url_then_hash
1. canonical_url 相同 → 保留權重高或時間新的
2. content_hash 相同 → 保留權重高或時間新的
"""

from typing import List, Dict, Tuple
from trend_miner.models import ItemRecord
import logging

logger = logging.getLogger(__name__)


def deduplicate_items(
    items: List[ItemRecord],
    strategy: str = "url_then_hash"
) -> Tuple[List[ItemRecord], Dict[str, int]]:
    """
    去重
    
    Args:
        items: 原始 items
        strategy: 去重策略 (目前僅支援 url_then_hash)
    
    Returns:
        (去重後的 items, 統計資訊)
    """
    if strategy != "url_then_hash":
        raise ValueError(f"Unsupported dedupe strategy: {strategy}")
    
    stats = {
        'original_count': len(items),
        'duplicates_by_url': 0,
        'duplicates_by_hash': 0,
        'final_count': 0
    }
    
    # Phase 1: URL 去重
    url_map: Dict[str, ItemRecord] = {}
    
    for item in items:
        canonical_url = item.canonical_url
        
        if canonical_url not in url_map:
            url_map[canonical_url] = item
        else:
            # 已存在，比較權重與時間
            existing = url_map[canonical_url]
            
            # 保留權重高的，若相同則保留時間新的
            if (item.source_weight > existing.source_weight or 
                (item.source_weight == existing.source_weight and 
                 item.published_at > existing.published_at)):
                url_map[canonical_url] = item
                stats['duplicates_by_url'] += 1
            else:
                stats['duplicates_by_url'] += 1
    
    url_deduped = list(url_map.values())
    logger.info(f"After URL dedupe: {len(url_deduped)} items ({stats['duplicates_by_url']} duplicates)")
    
    # Phase 2: Content hash 去重
    hash_map: Dict[str, ItemRecord] = {}
    
    for item in url_deduped:
        content_hash = item.content_hash
        
        if content_hash not in hash_map:
            hash_map[content_hash] = item
        else:
            # 已存在，比較權重與時間
            existing = hash_map[content_hash]
            
            if (item.source_weight > existing.source_weight or 
                (item.source_weight == existing.source_weight and 
                 item.published_at > existing.published_at)):
                hash_map[content_hash] = item
                stats['duplicates_by_hash'] += 1
            else:
                stats['duplicates_by_hash'] += 1
    
    final_items = list(hash_map.values())
    stats['final_count'] = len(final_items)
    
    logger.info(f"After hash dedupe: {stats['final_count']} items ({stats['duplicates_by_hash']} duplicates)")
    logger.info(f"Total dedupe: {stats['original_count']} -> {stats['final_count']} " +
                f"({stats['duplicates_by_url'] + stats['duplicates_by_hash']} duplicates)")
    
    return final_items, stats
