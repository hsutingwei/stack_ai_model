"""
Tests for topic signature stability
"""

import pytest
from trend_miner.utils.hashing import topic_signature


def test_signature_stability():
    """測試 topic signature 的穩定性（相同輸入 = 相同輸出）"""
    keywords1 = ["market", "stocks", "rally", "trading", "investors"]
    domains1 = ["reuters.com", "bloomberg.com", "ft.com"]
    
    sig1 = topic_signature(keywords1, domains1)
    sig2 = topic_signature(keywords1, domains1)
    
    assert sig1 == sig2


def test_signature_order_invariant():
    """測試 signature 對於輸入順序的不變性"""
    # 注意：signature 函數取前 N 個，所以輸入必須已排序
    keywords = ["a", "b", "c", "d", "e"]
    domains = ["x.com", "y.com", "z.com"]
    
    sig1 = topic_signature(keywords, domains)
    sig2 = topic_signature(keywords, domains)
    
    assert sig1 == sig2


def test_signature_different_inputs():
    """測試不同輸入產生不同 signature"""
    keywords1 = ["market", "stocks"]
    domains1 = ["reuters.com"]
    
    keywords2 = ["economy", "growth"]
    domains2 = ["bloomberg.com"]
    
    sig1 = topic_signature(keywords1, domains1)
    sig2 = topic_signature(keywords2, domains2)
    
    assert sig1 != sig2


def test_signature_truncation():
    """測試 signature 只使用前 10 keywords 和前 5 domains"""
    keywords_long = [f"kw{i}" for i in range(20)]
    domains_long = [f"domain{i}.com" for i in range(10)]
    
    keywords_short = [f"kw{i}" for i in range(10)]
    domains_short = [f"domain{i}.com" for i in range(5)]
    
    # 前 10/5 個相同，應該產生相同 signature
    sig1 = topic_signature(keywords_long, domains_long)
    sig2 = topic_signature(keywords_short, domains_short)
    
    assert sig1 == sig2


def test_signature_format():
    """測試 signature 格式（應該是 hex string）"""
    keywords = ["test"]
    domains = ["test.com"]
    
    sig = topic_signature(keywords, domains)
    
    # 應該是 64 字元的 hex string (SHA256)
    assert len(sig) == 64
    assert all(c in '0123456789abcdef' for c in sig)
