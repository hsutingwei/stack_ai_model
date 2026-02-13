"""
Core data models for L1 Trend Miner

Define ItemRecord and TopicRecord as the output contract for 1.5-layer Gate.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class ItemRecord(BaseModel):
    """
    Item-level metadata (每篇文章一筆)
    
    這是 L1 -> Gate 的核心契約。每個欄位都是 Gate 計算所需。
    """
    item_id: str = Field(..., description="穩定 ID: hash(canonical_url)")
    run_id: str = Field(..., description="執行 ID")
    canonical_url: str = Field(..., description="正規化 URL (移除追蹤參數)")
    publisher_domain: str = Field(..., description="eTLD+1 domain (e.g. reuters.com)")
    published_at: datetime = Field(..., description="發布時間 (UTC tz-aware)")
    fetched_at: datetime = Field(..., description="抓取時間 (UTC tz-aware)")
    
    source_name: str = Field(..., description="Feed 名稱")
    source_weight: float = Field(default=1.0, description="Feed 權重")
    
    title: str = Field(..., description="文章標題")
    summary: str = Field(default="", description="RSS snippet")
    has_summary: bool = Field(..., description="標記原始 RSS 是否有 summary")
    text_len: int = Field(..., description="title + summary 長度")
    
    content_hash: str = Field(..., description="sha256(normalized(title + summary))")
    topic_id: int = Field(..., description="BERTopic ID (-1 為 noise)")
    topic_signature: str = Field(..., description="穩定 Topic ID")
    
    json_payload: Dict[str, Any] = Field(default_factory=dict, description="原始 entry 必要欄位")
    
    class Config:
        json_schema_extra = {
            "example": {
                "item_id": "abc123",
                "run_id": "run_20260213_001",
                "canonical_url": "https://finance.yahoo.com/news/market-update",
                "publisher_domain": "yahoo.com",
                "published_at": "2026-02-13T10:00:00Z",
                "fetched_at": "2026-02-13T11:00:00Z",
                "source_name": "Yahoo Finance RSS",
                "source_weight": 0.8,
                "title": "Market Update: Stocks Rise",
                "summary": "Major indices posted gains...",
                "has_summary": True,
                "text_len": 150,
                "content_hash": "def456",
                "topic_id": 3,
                "topic_signature": "sig789"
            }
        }


class TopicBucket(BaseModel):
    """時間桶統計"""
    bucket_start: str = Field(..., description="UTC 日期 YYYY-MM-DD")
    count: int = Field(..., description="該時間桶內的文章數")


class RepresentativeItem(BaseModel):
    """代表性文章"""
    url: str
    title: str
    domain: str
    published_at: datetime
    summary: str


class TopicRecord(BaseModel):
    """
    Topic-level aggregation (每個主題一筆)
    
    預計算聚合指標，供 Gate 快速讀取。
    """
    run_id: str = Field(..., description="執行 ID")
    topic_id: int = Field(..., description="BERTopic ID")
    topic_signature: str = Field(..., description="sha256(json.dumps({keywords[:10], domains[:5]}, sort_keys=True))")
    
    top_keywords: List[str] = Field(default_factory=list, description="BERTopic c-TF-IDF 關鍵字")
    
    # 聚合統計
    topic_volume: int = Field(..., description="文章數")
    unique_domains: int = Field(..., description="相異 publisher 數")
    avg_source_weight: float = Field(..., description="平均來源權重")
    duplicate_ratio: float = Field(..., description="1 - (unique_hash / topic_volume)")
    
    # 時間序列
    counts_by_bucket: List[TopicBucket] = Field(default_factory=list, description="UTC Daily 統計")
    first_seen_at: Optional[datetime] = Field(None, description="最早文章時間")
    last_seen_at: Optional[datetime] = Field(None, description="最晚文章時間")
    
    # 代表性內容
    representative_items: List[RepresentativeItem] = Field(default_factory=list, description="3-5 篇代表文章")
    
    # L1 Preliminary Signal (不是最終 Gate Score)
    narrative_signal_score: Optional[float] = Field(None, description="L1 初步評分 (0-100)")
    
    json_payload: Dict[str, Any] = Field(default_factory=dict, description="預留欄位")
    
    class Config:
        json_schema_extra = {
            "example": {
                "run_id": "run_20260213_001",
                "topic_id": 3,
                "topic_signature": "sig789",
                "top_keywords": ["market", "stocks", "rally"],
                "topic_volume": 45,
                "unique_domains": 12,
                "avg_source_weight": 0.85,
                "duplicate_ratio": 0.15,
                "counts_by_bucket": [
                    {"bucket_start": "2026-02-12", "count": 20},
                    {"bucket_start": "2026-02-13", "count": 25}
                ],
                "narrative_signal_score": 75.5
            }
        }


class RunMetadata(BaseModel):
    """執行期中繼資料"""
    run_id: str
    generated_at: datetime
    lookback_days: int
    config_hash: str
    status: str = Field(default="running")
    
    # 統計資訊
    stats: Dict[str, Any] = Field(default_factory=dict, description="抓取數、去重數、topic 數等")
    
    class Config:
        json_schema_extra = {
            "example": {
                "run_id": "run_20260213_001",
                "generated_at": "2026-02-13T11:00:00Z",
                "lookback_days": 7,
                "config_hash": "abc123",
                "status": "completed",
                "stats": {
                    "fetched_count": 500,
                    "deduped_count": 450,
                    "topic_count": 15,
                    "noise_ratio": 0.12,
                    "missing_summary_ratio": 0.08
                }
            }
        }
