"""
Postgres storage backend with composite primary keys

Items: (run_id, item_id) 避免跨 run 衝突
Topics: (run_id, topic_signature)
"""

import os
from typing import List, Optional
import logging
import psycopg2
from psycopg2.extras import execute_values
import json

from trend_miner.models import ItemRecord, TopicRecord, RunMetadata

logger = logging.getLogger(__name__)


class PostgresStore:
    """Postgres 儲存後端"""
    
    def __init__(self, dsn: str):
        """
        初始化 PostgresStore
        
        Args:
            dsn: Postgres connection string
        """
        self.dsn = dsn
        self.conn = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """建立資料庫連線"""
        try:
            self.conn = psycopg2.connect(self.dsn)
            logger.info("Connected to Postgres")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise
    
    def _create_tables(self):
        """建立資料表 (含 composite PK)"""
        ddl = """
        CREATE TABLE IF NOT EXISTS runs (
            run_id VARCHAR(64) PRIMARY KEY,
            generated_at TIMESTAMP WITH TIME ZONE NOT NULL,
            lookback_days INTEGER NOT NULL,
            config_hash VARCHAR(32) NOT NULL,
            status VARCHAR(32) NOT NULL,
            stats_json JSONB
        );
        
        CREATE TABLE IF NOT EXISTS items (
            run_id VARCHAR(64) NOT NULL,
            item_id VARCHAR(32) NOT NULL,
            canonical_url TEXT NOT NULL,
            content_hash VARCHAR(64) NOT NULL,
            published_at TIMESTAMP WITH TIME ZONE NOT NULL,
            publisher_domain VARCHAR(256) NOT NULL,
            source_name VARCHAR(256) NOT NULL,
            source_weight FLOAT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT,
            has_summary BOOLEAN NOT NULL,
            text_len INTEGER NOT NULL,
            topic_id INTEGER NOT NULL,
            topic_signature VARCHAR(64) NOT NULL,
            jsonb_payload JSONB,
            PRIMARY KEY (run_id, item_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS topics (
            run_id VARCHAR(64) NOT NULL,
            topic_signature VARCHAR(64) NOT NULL,
            topic_id INTEGER NOT NULL,
            topic_volume INTEGER NOT NULL,
            unique_domains INTEGER NOT NULL,
            avg_source_weight FLOAT NOT NULL,
            duplicate_ratio FLOAT NOT NULL,
            counts_by_bucket_json JSONB,
            top_keywords_json JSONB,
            representative_items_json JSONB,
            narrative_signal_score FLOAT,
            jsonb_payload JSONB,
            PRIMARY KEY (run_id, topic_signature),
            FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );
        
        CREATE INDEX IF NOT EXISTS idx_items_canonical_url ON items(canonical_url);
        CREATE INDEX IF NOT EXISTS idx_items_content_hash ON items(content_hash);
        CREATE INDEX IF NOT EXISTS idx_items_published_at ON items(published_at);
        CREATE INDEX IF NOT EXISTS idx_topics_topic_id ON topics(topic_id);
        CREATE INDEX IF NOT EXISTS idx_topics_score ON topics(narrative_signal_score);
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(ddl)
            self.conn.commit()
            logger.info("Tables created/verified")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            self.conn.rollback()
            raise
    
    def write_run(self, run_meta: RunMetadata) -> None:
        """寫入 run metadata"""
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
                    run_meta.run_id,
                    run_meta.generated_at,
                    run_meta.lookback_days,
                    run_meta.config_hash,
                    run_meta.status,
                    json.dumps(run_meta.stats)
                ))
            self.conn.commit()
            logger.info(f"Written run: {run_meta.run_id}")
        except Exception as e:
            logger.error(f"Failed to write run: {e}")
            self.conn.rollback()
            raise
    
    def write_items(self, items: List[ItemRecord], run_id: str) -> None:
        """寫入 items (batch insert)"""
        if not items:
            return
        
        sql = """
        INSERT INTO items (
            run_id, item_id, canonical_url, content_hash, published_at,
            publisher_domain, source_name, source_weight, title, summary,
            has_summary, text_len, topic_id, topic_signature, jsonb_payload
        ) VALUES %s
        ON CONFLICT (run_id, item_id) DO NOTHING
        """
        
        values = [
            (
                item.run_id, item.item_id, item.canonical_url, item.content_hash,
                item.published_at, item.publisher_domain, item.source_name,
                item.source_weight, item.title, item.summary, item.has_summary,
                item.text_len, item.topic_id, item.topic_signature,
                json.dumps(item.json_payload)
            )
            for item in items
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values)
            self.conn.commit()
            logger.info(f"Written {len(items)} items")
        except Exception as e:
            logger.error(f"Failed to write items: {e}")
            self.conn.rollback()
            raise
    
    def write_topics(self, topics: List[TopicRecord], run_id: str) -> None:
        """寫入 topics (batch insert)"""
        if not topics:
            return
        
        sql = """
        INSERT INTO topics (
            run_id, topic_signature, topic_id, topic_volume, unique_domains,
            avg_source_weight, duplicate_ratio, counts_by_bucket_json,
            top_keywords_json, representative_items_json, narrative_signal_score,
            jsonb_payload
        ) VALUES %s
        ON CONFLICT (run_id, topic_signature) DO UPDATE SET
            topic_volume = EXCLUDED.topic_volume,
            narrative_signal_score = EXCLUDED.narrative_signal_score
        """
        
        values = [
            (
                topic.run_id, topic.topic_signature, topic.topic_id, topic.topic_volume,
                topic.unique_domains, topic.avg_source_weight, topic.duplicate_ratio,
                json.dumps([b.model_dump() for b in topic.counts_by_bucket]),
                json.dumps(topic.top_keywords),
                json.dumps([r.model_dump() for r in topic.representative_items], default=str),
                topic.narrative_signal_score,
                json.dumps(topic.json_payload)
            )
            for topic in topics
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values)
            self.conn.commit()
            logger.info(f"Written {len(topics)} topics")
        except Exception as e:
            logger.error(f"Failed to write topics: {e}")
            self.conn.rollback()
            raise
    
    def close(self):
        """關閉連線"""
        if self.conn:
            self.conn.close()
            logger.info("Postgres connection closed")
