-- L1 Trend Miner PostgreSQL Schema
-- 初始化資料庫結構

-- ============================================================
-- 1. runs 表：記錄每次執行的 metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    generated_at TIMESTAMPTZ NOT NULL,
    lookback_days INT NOT NULL,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    stats_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_runs_generated_at ON runs(generated_at);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

-- ============================================================
-- 2. items 表：Item-level metadata (每篇文章一筆)
-- ============================================================
CREATE TABLE IF NOT EXISTS items (
    run_id UUID NOT NULL,
    item_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    publisher_domain TEXT,
    source_name TEXT,
    source_weight FLOAT,
    title TEXT,
    summary TEXT,
    has_summary BOOLEAN,
    text_len INT,
    topic_id INT,
    topic_signature TEXT,
    json_payload JSONB,
    PRIMARY KEY (run_id, item_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

-- 索引：用於去重、topic 查詢、時間序列分析
CREATE INDEX IF NOT EXISTS idx_items_content_hash ON items(content_hash);
CREATE INDEX IF NOT EXISTS idx_items_topic_signature ON items(topic_signature);
CREATE INDEX IF NOT EXISTS idx_items_canonical_url ON items(canonical_url);
CREATE INDEX IF NOT EXISTS idx_items_published_at ON items(published_at);

-- ============================================================
-- 3. topics 表：Topic-level aggregation
-- ============================================================
CREATE TABLE IF NOT EXISTS topics (
    run_id UUID NOT NULL,
    topic_signature TEXT NOT NULL,
    topic_id INT,
    topic_volume INT,
    unique_domains INT,
    avg_source_weight FLOAT,
    duplicate_ratio FLOAT,
    narrative_signal_score FLOAT,
    top_keywords JSONB,
    representative_items JSONB,
    scoring_breakdown JSONB,
    json_payload JSONB,
    PRIMARY KEY (run_id, topic_signature),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

-- 索引：用於 Gate 查詢、TopK 排序
CREATE INDEX IF NOT EXISTS idx_topics_score ON topics(narrative_signal_score DESC);
CREATE INDEX IF NOT EXISTS idx_topics_volume ON topics(topic_volume DESC);
CREATE INDEX IF NOT EXISTS idx_topics_topic_id ON topics(topic_id);

-- ============================================================
-- 4. topic_buckets 表：時間桶統計 (UTC Daily/Hourly)
-- ============================================================
CREATE TABLE IF NOT EXISTS topic_buckets (
    run_id UUID NOT NULL,
    topic_signature TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    count INT NOT NULL,
    PRIMARY KEY (run_id, topic_signature, bucket_start),
    FOREIGN KEY (run_id, topic_signature)
        REFERENCES topics(run_id, topic_signature)
        ON DELETE CASCADE
);

-- 索引：用於 velocity/THA 計算
CREATE INDEX IF NOT EXISTS idx_topic_buckets_bucket_start ON topic_buckets(bucket_start);

-- ============================================================
-- Verification Queries
-- ============================================================

-- 檢查表是否建立成功
-- SELECT table_name FROM information_schema.tables 
-- WHERE table_schema = 'public' AND table_name IN ('runs', 'items', 'topics', 'topic_buckets');

-- 檢查索引
-- SELECT indexname FROM pg_indexes WHERE tablename IN ('runs', 'items', 'topics', 'topic_buckets');
