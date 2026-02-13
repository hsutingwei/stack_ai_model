"""
Configuration schemas using Pydantic

定義完整的配置結構，包含 RSS feeds、BERTopic 參數、Memory Layer 設定等。
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field
import os


class RSSFeedConfig(BaseModel):
    """個別 RSS feed 設定"""
    name: str = Field(..., description="Feed 名稱")
    url: str = Field(..., description="RSS feed URL")
    weight: float = Field(default=1.0, description="權重 (0-1)")
    category: Optional[str] = Field(None, description="分類")
    market: Optional[str] = Field(None, description="市場: US|TW|GLOBAL")
    language: Optional[str] = Field(None, description="語言")


class BERTopicConfig(BaseModel):
    """BERTopic 參數配置"""
    embedding_model: str = Field(
        default="paraphrase-multilingual-MiniLM-L12-v2",
        description="Sentence transformers 模型名稱"
    )
    min_topic_size: int = Field(default=15, description="最小 topic 大小")
    nr_topics: Optional[int] = Field(None, description="Topic 數量 (None=auto)")
    top_n_words: int = Field(default=15, description="每個 topic 的關鍵字數")
    calculate_probabilities: bool = Field(default=False, description="是否計算機率 (成本高)")
    
    # UMAP 參數
    umap_n_neighbors: int = Field(default=15, description="UMAP n_neighbors")
    umap_n_components: int = Field(default=5, description="UMAP n_components")
    umap_min_dist: float = Field(default=0.0, description="UMAP min_dist")
    umap_metric: str = Field(default="cosine", description="UMAP metric")
    umap_random_state: int = Field(default=42, description="UMAP random state")
    
    # HDBSCAN 參數
    hdbscan_min_cluster_size: Optional[int] = Field(None, description="HDBSCAN min_cluster_size (若 None 則=min_topic_size)")
    hdbscan_min_samples: Optional[int] = Field(None, description="HDBSCAN min_samples (若 None 則=min_topic_size//2)")
    hdbscan_metric: str = Field(default="euclidean", description="HDBSCAN metric")


class MemoryConfig(BaseModel):
    """記憶層後端設定"""
    backend: Literal["files", "postgres"] = Field(default="files", description="儲存後端")
    postgres_dsn: Optional[str] = Field(None, description="Postgres DSN (環境變數名稱)")
    memory_fallback: Literal["none", "files"] = Field(
        default="files",
        description="Postgres 失敗時的降級策略 (production 建議 none)"
    )
    write_runs: bool = Field(default=True, description="是否寫入 runs")
    write_items: bool = Field(default=True, description="是否寫入 items")
    write_topics: bool = Field(default=True, description="是否寫入 topics")


class TrendMinerConfig(BaseModel):
    """完整設定 schema"""
    # 基本設定
    run_timezone: str = Field(default="Asia/Taipei", description="執行時區")
    lookback_days: int = Field(default=7, description="回溯天數")
    max_items_per_feed: int = Field(default=50, description="每個 feed 最多抓取數")
    top_k_topics: int = Field(default=10, description="輸出 Top K topics")
    min_items_to_cluster: int = Field(default=30, description="最少聚類項目數")
    output_dir: str = Field(default="out", description="輸出目錄")
    
    # 穩定性設定 (Determinism)
    global_random_seed: int = Field(default=42, description="全局 random seed")
    tldextract_cache_dir: Optional[str] = Field(
        default=".tldextract_cache",
        description="tldextract cache 目錄 (確保 determinism)"
    )
    
    # Dedupe 策略
    dedupe_strategy: str = Field(default="url_then_hash", description="去重策略")
    
    # Noise 處理
    noise_handling: Literal["drop", "keep"] = Field(
        default="drop",
        description="BERTopic noise (topic -1) 處理方式"
    )
    
    # Embedding cache
    enable_embedding_cache: bool = Field(default=True, description="啟用 embedding cache")
    embedding_cache_dir: str = Field(default="memory/embeddings", description="Embedding cache 目錄")
    
    # 語言偵測 (可選)
    enable_language_detect: bool = Field(default=False, description="啟用語言偵測")
    
    # Bucket 設定
    bucket_interval: Literal["daily", "hourly"] = Field(
        default="daily",
        description="時間桶間隔 (Gate velocity 計算用)"
    )
    
    # Feeds
    rss_feeds: List[RSSFeedConfig] = Field(default_factory=list, description="RSS feeds 清單")
    
    # Watchlist (可選)
    watchlist_tickers: List[str] = Field(default_factory=list, description="關注股票代碼")
    watchlist_keywords: List[str] = Field(default_factory=list, description="關注關鍵字")
    
    # BERTopic
    bertopic: BERTopicConfig = Field(default_factory=BERTopicConfig, description="BERTopic 參數")
    
    # Memory Layer
    memory: MemoryConfig = Field(default_factory=MemoryConfig, description="記憶層設定")
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "TrendMinerConfig":
        """從 YAML 檔案載入設定"""
        import yaml
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def get_postgres_dsn(self) -> Optional[str]:
        """取得 Postgres DSN (從環境變數)"""
        if self.memory.postgres_dsn:
            return os.environ.get(self.memory.postgres_dsn)
        return None
