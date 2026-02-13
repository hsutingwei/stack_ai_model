"""
Tests for deduplication logic
"""

import pytest
from datetime import datetime, timezone

from trend_miner.models import ItemRecord
from trend_miner.processing.dedupe import deduplicate_items


def create_test_item(
    item_id: str,
    canonical_url: str,
    content_hash: str,
    weight: float = 1.0,
    published_at: datetime = None
) -> ItemRecord:
    """Helper to create test item"""
    if published_at is None:
        published_at = datetime.now(timezone.utc)
    
    return ItemRecord(
        item_id=item_id,
        run_id="test_run",
        canonical_url=canonical_url,
        publisher_domain="example.com",
        published_at=published_at,
        fetched_at=datetime.now(timezone.utc),
        source_name="Test Feed",
        source_weight=weight,
        title="Test Title",
        summary="Test Summary",
        has_summary=True,
        text_len=100,
        content_hash=content_hash,
        topic_id=-1,
        topic_signature=""
    )


def test_dedupe_by_url():
    """測試 URL 去重"""
    item1 = create_test_item("id1", "https://example.com/article1", "hash1", weight=0.8)
    item2 = create_test_item("id2", "https://example.com/article1", "hash2", weight=0.9)  # 同 URL, 高權重
    
    items = [item1, item2]
    deduped, stats = deduplicate_items(items)
    
    assert len(deduped) == 1
    assert deduped[0].source_weight == 0.9  # 保留高權重
    assert stats['duplicates_by_url'] == 1


def test_dedupe_by_hash():
    """測試 content hash 去重"""
    item1 = create_test_item("id1", "https://example.com/article1", "hash_same", weight=0.8)
    item2 = create_test_item("id2", "https://example.com/article2", "hash_same", weight=0.9)  # 不同 URL, 同 hash
    
    items = [item1, item2]
    deduped, stats = deduplicate_items(items)
    
    assert len(deduped) == 1
    assert deduped[0].source_weight == 0.9
    assert stats['duplicates_by_hash'] == 1


def test_dedupe_by_time():
    """測試時間優先（權重相同時）"""
    old_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new_time = datetime(2026, 2, 1, tzinfo=timezone.utc)
    
    item1 = create_test_item("id1", "https://example.com/article1", "hash1", weight=0.8, published_at=old_time)
    item2 = create_test_item("id2", "https://example.com/article1", "hash2", weight=0.8, published_at=new_time)
    
    items = [item1, item2]
    deduped, stats = deduplicate_items(items)
    
    assert len(deduped) == 1
    assert deduped[0].published_at == new_time  # 保留較新的


def test_no_duplicates():
    """測試沒有重複的情況"""
    item1 = create_test_item("id1", "https://example.com/article1", "hash1")
    item2 = create_test_item("id2", "https://example.com/article2", "hash2")
    item3 = create_test_item("id3", "https://example.com/article3", "hash3")
    
    items = [item1, item2, item3]
    deduped, stats = deduplicate_items(items)
    
    assert len(deduped) == 3
    assert stats['duplicates_by_url'] == 0
    assert stats['duplicates_by_hash'] == 0
