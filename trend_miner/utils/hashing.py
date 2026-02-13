"""Hashing utilities for content and topic signatures."""

import hashlib
import json
from typing import Dict, List, Any


def content_hash(title: str, summary: str) -> str:
    """
    產生 content hash
    
    Args:
        title: 標題
        summary: 摘要
    
    Returns:
        SHA256 hash (hex)
    """
    # 正規化：小寫、去除多餘空白
    normalized = f"{title.lower().strip()} {summary.lower().strip()}"
    normalized = " ".join(normalized.split())
    
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def url_hash(url: str) -> str:
    """
    產生 URL hash (作為 item_id)
    
    Args:
        url: canonical URL
    
    Returns:
        SHA256 hash (hex, 前 16 字元)
    """
    return hashlib.sha256(url.encode('utf-8')).hexdigest()[:16]


def topic_signature(keywords: List[str], domains: List[str]) -> str:
    """
    產生穩定的 topic signature
    
    Args:
        keywords: Top keywords (已排序)
        domains: Top publisher domains (已排序)
    
    Returns:
        SHA256 hash (hex)
    """
    # 確保穩定性：使用 sorted JSON dumps
    signature_input = {
        "kw": keywords[:10],  # 取前 10 個
        "pub": domains[:5]     # 取前 5 個
    }
    
    json_str = json.dumps(signature_input, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def config_hash(config_dict: Dict[str, Any]) -> str:
    """
    產生 config hash
    
    Args:
        config_dict: 設定字典
    
    Returns:
        SHA256 hash (hex, 前 16 字元)
    """
    # 排除會變動的欄位 (例如 output_dir)
    stable_keys = ['lookback_days', 'bertopic', 'rss_feeds', 'dedupe_strategy']
    stable_config = {k: config_dict.get(k) for k in stable_keys if k in config_dict}
    
    json_str = json.dumps(stable_config, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()[:16]
