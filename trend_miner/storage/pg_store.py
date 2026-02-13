"""
Postgres storage backend with automatic schema initialization

使用 psycopg2-binary，支援 transaction handling 與 bulk insert。
"""

import uuid
from typing import List
import logging
import psycopg2
from psycopg2.extras import execute_values
import json

from trend_miner.models import ItemRecord, TopicRecord, RunMetadata

logger = logging.getLogger(__name__)


class PostgresStore:
    """Postgres 儲存後端（不 fallback，fail fast）"""
    
    def __init__(self, dsn: str, auto_init_schema: bool = True):
        """
        初始化 PostgresStore
        
        Args:
            dsn: Postgres connection string
            auto_init_schema: 是否自動建立 schema
        """
        self.dsn = dsn
        self.conn = None
        self._connect()
        
        if auto_init_schema:
            self.init_schema()
    
    def _connect(self):
        """建立資料庫連線（連線失敗直接拋出異常，不 fallback）"""
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = False  # 使用 transaction
            logger.info("✓ Connected to Postgres")
        except Exception as e:
            logger.error(f"✗ Failed to connect to Postgres: {e}")
            raise RuntimeError(f"Postgres connection failed (no fallback): {e}")
    
    def init_schema(self):
        """初始化資料庫 schema（若表不存在則建立）"""
        # 讀取 init_trend_miner.sql
        import os
        from pathlib import Path
        
        # 找到專案根目錄的 SQL 檔案
        project_root = Path(__file__).parent.parent.parent
        sql_file = project_root / "init_trend_miner.sql"
        
        if not sql_file.exists():
            logger.warning(f"init_trend_miner.sql not found at {sql_file}, creating tables programatically")
            self._create_tables_programatically()
            return
        
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            with self.conn.cursor() as cur:
                cur.execute(sql_script)
            self.conn.commit()
            logger.info("✓ Schema initialized successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to initialize schema: {e}")
            raise
    
    def _create_tables_programatically(self):
        """程式化建立表（若 SQL 檔案不存在）"""
        ddl = """
        CREATE TABLE IF NOT EXISTS runs (
            run_id UUID PRIMARY KEY,
            generated_at TIMESTAMPTZ NOT NULL,
            lookback_days INT NOT NULL,
            config_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            stats_json JSONB
        );
        
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
        
        CREATE INDEX IF NOT EXISTS idx_items_content_hash ON items(content_hash);
        CREATE INDEX IF NOT EXISTS idx_items_topic_signature ON items(topic_signature);
        CREATE INDEX IF NOT EXISTS idx_topics_score ON topics(narrative_signal_score DESC);
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(ddl)
            self.conn.commit()
            logger.info("✓ Tables created programatically")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def save_run(self, run_meta: RunMetadata) -> None:
        """寫入 run metadata"""
        run_id_uuid = uuid.UUID(run_meta.run_id) if not isinstance(run_meta.run_id, uuid.UUID) else run_meta.run_id
        
        sql = """
        INSERT INTO runs (run_id, generated_at, lookback_days, config_hash, status, stats_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            stats_json = EXCLUDED.stats_json
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (
                    run_id_uuid,
                    run_meta.generated_at,
                    run_meta.lookback_days,
                    run_meta.config_hash,
                    run_meta.status,
                    json.dumps(run_meta.stats, default=str)
                ))
            self.conn.commit()
            logger.info(f"✓ Saved run: {run_meta.run_id}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save run: {e}")
            raise
    
    def save_items(self, items: List[ItemRecord]) -> None:
        """Bulk insert items"""
        if not items:
            return
        
        run_id_uuid = uuid.UUID(items[0].run_id)
        
        sql = """
        INSERT INTO items (
            run_id, item_id, canonical_url, content_hash, published_at,
            publisher_domain, source_name, source_weight, title, summary,
            has_summary, text_len, topic_id, topic_signature, json_payload
        ) VALUES %s
        ON CONFLICT (run_id, item_id) DO NOTHING
        """
        
        values = [
            (
                run_id_uuid, item.item_id, item.canonical_url, item.content_hash,
                item.published_at, item.publisher_domain, item.source_name,
                item.source_weight, item.title, item.summary, item.has_summary,
                item.text_len, item.topic_id, item.topic_signature,
                json.dumps(item.json_payload, default=str)
            )
            for item in items
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values)
            self.conn.commit()
            logger.info(f"✓ Saved {len(items)} items (bulk insert)")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save items: {e}")
            raise
    
    def save_topics(self, topics: List[TopicRecord]) -> None:
        """寫入 topics"""
        if not topics:
            return
        
        run_id_uuid = uuid.UUID(topics[0].run_id)
        
        sql = """
        INSERT INTO topics (
            run_id, topic_signature, topic_id, topic_volume, unique_domains,
            avg_source_weight, duplicate_ratio, narrative_signal_score,
            top_keywords, representative_items, scoring_breakdown, json_payload
        ) VALUES %s
        ON CONFLICT (run_id, topic_signature) DO UPDATE SET
            topic_volume = EXCLUDED.topic_volume,
            narrative_signal_score = EXCLUDED.narrative_signal_score
        """
        
        values = [
            (
                run_id_uuid, topic.topic_signature, topic.topic_id, topic.topic_volume,
                topic.unique_domains, topic.avg_source_weight, topic.duplicate_ratio,
                topic.narrative_signal_score,
                json.dumps(topic.top_keywords),
                json.dumps([r.model_dump() for r in topic.representative_items], default=str),
                json.dumps(topic.json_payload.get('score_breakdown', {})),
                json.dumps(topic.json_payload, default=str)
            )
            for topic in topics
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values)
            self.conn.commit()
            logger.info(f"✓ Saved {len(topics)} topics")
            
            # 同時寫入 topic_buckets
            self.save_topic_buckets(topics)
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save topics: {e}")
            raise
    
    def save_topic_buckets(self, topics: List[TopicRecord]) -> None:
        """寫入 topic_buckets（時間桶統計）"""
        if not topics:
            return
        
        run_id_uuid = uuid.UUID(topics[0].run_id)
        
        sql = """
        INSERT INTO topic_buckets (run_id, topic_signature, bucket_start, count)
        VALUES %s
        ON CONFLICT (run_id, topic_signature, bucket_start) DO UPDATE SET
            count = EXCLUDED.count
        """
        
        values = []
        for topic in topics:
            for bucket in topic.counts_by_bucket:
                values.append((
                    run_id_uuid,
                    topic.topic_signature,
                    bucket.bucket_start,
                    bucket.count
                ))
        
        if not values:
            return
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values)
            self.conn.commit()
            logger.info(f"✓ Saved {len(values)} topic buckets")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save topic buckets: {e}")
            raise
    
    def close(self):
        """關閉連線"""
        if self.conn:
            self.conn.close()
            logger.info("Postgres connection closed")
