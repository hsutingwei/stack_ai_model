# L1 Lightweight Trend Miner

L1 Lightweight Trend Miner 是一個基於 BERTopic 的趨勢挖掘系統，專為 1.5-layer Gate 設計的數據產生器。

## 特色

- ✅ **BERTopic 多語支援**：使用 `paraphrase-multilingual-MiniLM-L12-v2` 處理中英混合內容
- ✅ **穩定性保證**：固定 random seed、cached tldextract、確定性 topic signature
- ✅ **Gate-Ready Schema**：完整的 ItemRecord 和 TopicRecord，包含所有 Gate 計算所需欄位
- ✅ **雙儲存後端**：File-based (開發) + Postgres (生產)，支援 composite PK 避免跨 run 衝突
- ✅ **嚴格限制**：僅使用 RSS title + summary，不抓取全文
- ✅ **時間序列支援**：UTC Daily/Hourly buckets for velocity 計算

## 安裝

```bash
cd c:\yves\ai_project
pip install -e .
```

### 依賴項

主要依賴：
- `bertopic>=0.16.0`
- `sentence-transformers>=2.2.0`
- `hdbscan>=0.8.0`
- `umap-learn>=0.5.0`
- `feedparser>=6.0.0`
- `pydantic>=2.0.0`
- `tldextract>=5.0.0`
- `psycopg2-binary>=2.9.0` (Postgres 使用)

## 快速開始

### 1. 產生設定檔

```bash
python -m trend_miner init-config --out my_config.yaml
```

### 2. 編輯設定檔

編輯 `my_config.yaml`，至少設定 RSS feeds：

```yaml
rss_feeds:
  - name: "Reuters Business"
    url: "https://www.reutersagency.com/feed/?taxonomy=best-topics&post_type=best"
    weight: 1.0
    category: "news"
    market: "GLOBAL"
```

### 3. 執行

```bash
python -m trend_miner run --config my_config.yaml
```

## 設定說明

### 核心參數

```yaml
# 穩定性設定（必須）
global_random_seed: 42              # 全局 random seed
tldextract_cache_dir: ".tldextract_cache"  # Domain 解析 cache

# 基本設定
lookback_days: 7                    # 回溯天數
min_items_to_cluster: 30            # 最少聚類項目數
top_k_topics: 10                    # 輸出 Top K topics
bucket_interval: "daily"            # 時間桶: daily | hourly

# Noise 處理
noise_handling: "drop"              # drop | keep (BERTopic noise)
```

### BERTopic 參數

```yaml
bertopic:
  embedding_model: "paraphrase-multilingual-MiniLM-L12-v2"
  min_topic_size: 15
  umap_random_state: 42             # 確保 determinism
  umap_n_neighbors: 15
  hdbscan_min_cluster_size: null    # null = min_topic_size
```

### Storage Layer

L1 支援雙儲存後端：

#### File Mode (預設，用於開發)

```yaml
storage:
  mode: "file"
  postgres_dsn: null
```

產出：`memory/runs/`, `memory/items/`, `memory/topics/`

#### Postgres Mode (生產環境)

##### 1. 建立資料庫

```bash
# 建立資料庫
createdb trend_miner

# 初始化 schema
psql -d trend_miner -f init_trend_miner.sql
```

##### 2. 設定 config

```yaml
storage:
  mode: "postgres"
  postgres_dsn: "postgresql://user:password@localhost:5432/trend_miner"
```

##### 3. Optional: 同時匯出檔案 (Debug)

```yaml
export:
  enable_file_dump: true
  output_dir: "exports"
```

##### 4. 驗證資料

```bash
# 檢查 runs
psql -d trend_miner -c "SELECT run_id, status, generated_at FROM runs ORDER BY generated_at DESC LIMIT 5;"

# 檢查 items 數量
psql -d trend_miner -c "SELECT run_id, COUNT(*) as item_count FROM items GROUP BY run_id;"

# 檢查 topics (Top 10 by score)
psql -d trend_miner -c "SELECT topic_signature, topic_volume, narrative_signal_score FROM topics ORDER BY narrative_signal_score DESC LIMIT 10;"

# 檢查 topic_buckets
psql -d trend_miner -c "SELECT run_id, COUNT(DISTINCT topic_signature) as topic_count, COUNT(*) as bucket_count FROM topic_buckets GROUP BY run_id;"
```

##### Schema 說明

- **runs**: 每次執行的 metadata (UUID primary key)
- **items**: Item-level data，composite PK `(run_id, item_id)`
- **topics**: Topic-level aggregation，composite PK `(run_id, topic_signature)`
- **topic_buckets**: 時間桶統計，用於 velocity/THA 計算



## 輸出格式

### topics.json

```json
[
  {
    "run_id": "run_20260213_001",
    "topic_id": 3,
    "topic_signature": "abc123...",
    "top_keywords": ["market", "stocks", "rally"],
    "topic_volume": 45,
    "unique_domains": 12,
    "avg_source_weight": 0.85,
    "duplicate_ratio": 0.15,
    "counts_by_bucket": [
      {"bucket_start": "2026-02-12", "count": 20},
      {"bucket_start": "2026-02-13", "count": 25}
    ],
    "representative_items": [...],
    "narrative_signal_score": 75.5
  }
]
```

### items.jsonl

每行一個 JSON 物件：

```json
{"item_id": "abc123", "run_id": "run_20260213_001", "canonical_url": "https://...", "has_summary": true, "text_len": 150, ...}
```

## 常見問題

### RSS Feed 失敗

如果某個 feed 失敗，系統會記錄錯誤並繼續處理其他 feeds。檢查 log：

```
ERROR - Error fetching feed Reuters: ...
```

### tldextract 卡住

首次執行會下載 Public Suffix List。確保：
1. 網路連線正常
2. 或使用內建 snapshot (已在 `url_normalize.py` 中處理)

### Postgres 連線失敗

**Postgres mode 採用 fail-fast 策略，連線失敗會直接報錯**：

```
RuntimeError: Postgres connection failed (no fallback): ...
```

解決方式：
1. 檢查 `postgres_dsn` 是否正確
2. 確認 Postgres 服務運行中
3. 驗證帳號密碼與資料庫存在
4. 若需 debug，改用 `storage.mode: "file"`

### Items 數量太少，無法聚類

如果 items < `min_items_to_cluster`，系統會將所有 items 視為單一 topic (degraded mode)。

## Milestone 交付

- ✅ Milestone 1: RSS Collection + Deduplication + BERTopic Clustering
- ✅ Milestone 2: Topic Aggregation + Stable Signatures
- ✅ Milestone 3: Memory Layer (Files + Postgres with Composite PK)
- ⏳ Milestone 4: Embedding Cache (optional optimization)

## 測試

```bash
pytest tests/
```

## 後續擴充點（v1 不做）

- Embedding cache layer (降低成本)
- Language detection (langdetect)
- Real-time streaming mode
- Full-text fetching (留給 L2)
- Deep-dive analysis (留給 L2)

## License

MIT
