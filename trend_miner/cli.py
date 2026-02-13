"""
CLI: Command Line Interface for Trend Miner

支援 init-config 和 run 命令。
"""

import click
import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import uuid

from trend_miner.config import TrendMinerConfig
from trend_miner.models import RunMetadata
from trend_miner.collectors.rss import collect_all_feeds
from trend_miner.processing.dedupe import deduplicate_items
from trend_miner.processing.bertopic_model import cluster_items, get_topic_keywords
from trend_miner.processing.topic_aggregate import aggregate_topics
from trend_miner.processing.scoring import calculate_narrative_scores
from trend_miner.storage.file_store import FileStore
from trend_miner.storage.pg_store import PostgresStore
from trend_miner.utils import hashing
from trend_miner.utils.time import utcnow, format_iso8601

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """L1 Lightweight Trend Miner CLI"""
    pass


@cli.command()
@click.option('--out', default='config.example.yaml', help='Output config file path')
def init_config(out: str):
    """產生範本設定檔"""
    
    # 讀取現有的 example config (如果存在)
    example_path = Path(__file__).parent.parent / 'config.example.yaml'
    
    if example_path.exists():
        with open(example_path, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        # Minimal fallback
        content = """# L1 Trend Miner Configuration
run_timezone: "Asia/Taipei"
lookback_days: 7
rss_feeds: []
"""
    
    # 寫入輸出檔案
    with open(out, 'w', encoding='utf-8') as f:
        f.write(content)
    
    click.echo(f"✓ Config file created: {out}")
    click.echo(f"  Edit this file and run: python -m trend_miner run --config {out}")


@cli.command()
@click.option('--config', required=True, help='Config YAML file path')
def run(config: str):
    """執行一次 Trend Miner"""
    
    click.echo("=" * 60)
    click.echo("L1 Lightweight Trend Miner (BERTopic + Gate-Ready)")
    click.echo("=" * 60)
    
    # 讀取設定
    logger.info(f"Loading config: {config}")
    cfg = TrendMinerConfig.from_yaml(config)
    
    # Generate run_id
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{timestamp}_{uuid.uuid4().hex[:8]}"
    logger.info(f"Run ID: {run_id}")
    
    # Create output directory
    output_dir = Path(cfg.output_dir) / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize storage
    storage = initialize_storage(cfg)
    
    # Create run metadata
    run_meta = RunMetadata(
        run_id=run_id,
        generated_at=utcnow(),
        lookback_days=cfg.lookback_days,
        config_hash=hashing.config_hash(cfg.model_dump()),
        status="running",
        stats={}
    )
    
    try:
        # Step 1: Collect from RSS feeds
        logger.info("=" * 40)
        logger.info("STEP 1: Collecting from RSS feeds")
        logger.info("=" * 40)
        
        items = collect_all_feeds(
            cfg.rss_feeds,
            run_id,
            max_items_per_feed=cfg.max_items_per_feed,
            lookback_days=cfg.lookback_days,
            run_timezone=cfg.run_timezone,
            tldextract_cache_dir=cfg.tldextract_cache_dir
        )
        
        click.echo(f"✓ Collected {len(items)} items from {len(cfg.rss_feeds)} feeds")
        
        if len(items) == 0:
            logger.warning("No items collected, exiting")
            return
        
        # Step 2: Deduplication
        logger.info("=" * 40)
        logger.info("STEP 2: Deduplication")
        logger.info("=" * 40)
        
        items, dedupe_stats = deduplicate_items(items, cfg.dedupe_strategy)
        click.echo(f"✓ After dedupe: {dedupe_stats['final_count']} items " +
                  f"({dedupe_stats['duplicates_by_url'] + dedupe_stats['duplicates_by_hash']} duplicates)")
        
        # Step 3: BERTopic Clustering
        logger.info("=" * 40)
        logger.info("STEP 3: BERTopic Clustering")
        logger.info("=" * 40)
        
        items, cluster_stats = cluster_items(
            items,
            cfg.bertopic,
            cfg.global_random_seed,
            cfg.min_items_to_cluster,
            cfg.noise_handling
        )
        
        click.echo(f"✓ Clustering complete: {cluster_stats['n_topics']} topics, " +
                  f"{cluster_stats['noise_count']} noise ({cluster_stats['noise_ratio']:.1%})")
        
        # Get topic keywords
        topic_keywords = get_topic_keywords(items, cfg.bertopic, cfg.global_random_seed)
        
        # Step 4: Topic Aggregation
        logger.info("=" * 40)
        logger.info("STEP 4: Topic Aggregation")
        logger.info("=" * 40)
        
        topics = aggregate_topics(items, topic_keywords, cfg.bucket_interval)
        click.echo(f"✓ Created {len(topics)} TopicRecords")
        
        # Step 5: Scoring
        logger.info("=" * 40)
        logger.info("STEP 5: Narrative Scoring")
        logger.info("=" * 40)
        
        topics = calculate_narrative_scores(
            topics,
            cfg.watchlist_keywords,
            cfg.watchlist_tickers
        )
        
        # Sort topics by score
        topics.sort(key=lambda t: t.narrative_signal_score or 0, reverse=True)
        
        # TopK
        top_topics = topics[:cfg.top_k_topics]
        click.echo(f"✓ Top {len(top_topics)} topics scored")
        
        # Step 6: Write outputs
        logger.info("=" * 40)
        logger.info("STEP 6: Writing outputs")
        logger.info("=" * 40)
        
        # Write to files (always)
        write_output_files(output_dir, run_id, items, top_topics)
        click.echo(f"✓ Written outputs to {output_dir}")
        
        # Write to storage
        run_meta.status = "completed"
        run_meta.stats = {
            'fetched_count': dedupe_stats['original_count'],
            'deduped_count': dedupe_stats['final_count'],
            'topic_count': cluster_stats['n_topics'],
            'noise_ratio': cluster_stats['noise_ratio'],
            'missing_summary_ratio': sum(1 for item in items if not item.has_summary) / len(items) if items else 0
        }
        
        if storage:
            storage.save_run(run_meta)
            storage.save_items(items)
            storage.save_topics(top_topics)
            click.echo(f"✓ Written to storage backend: {cfg.storage.mode}")
        
        # Optional file export (postgres mode)
        if cfg.storage.mode == "postgres" and cfg.export.enable_file_dump:
            export_dir = Path(cfg.export.output_dir) / run_id
            write_output_files(export_dir, run_id, items, top_topics)
            click.echo(f"✓ Exported files to {export_dir}")
        
        # Summary
        click.echo("\n" + "=" * 60)
        click.echo("RUN SUMMARY")
        click.echo("=" * 60)
        click.echo(f"Run ID: {run_id}")
        click.echo(f"Fetched: {run_meta.stats['fetched_count']} items")
        click.echo(f"After dedupe: {run_meta.stats['deduped_count']} items")
        click.echo(f"Topics: {run_meta.stats['topic_count']}")
        click.echo(f"Noise ratio: {run_meta.stats['noise_ratio']:.1%}")
        click.echo(f"Missing summary: {run_meta.stats['missing_summary_ratio']:.1%}")
        click.echo(f"\nTop 5 Topics:")
        for i, topic in enumerate(top_topics[:5], 1):
            kw_str = ', '.join(topic.top_keywords[:5])
            click.echo(f"  {i}. Score={topic.narrative_signal_score:.1f}, " +
                      f"Volume={topic.topic_volume}, Keywords=[{kw_str}]")
        
    except Exception as e:
        logger.error(f"Run failed: {e}", exc_info=True)
        run_meta.status = "failed"
        run_meta.stats['error'] = str(e)
        if storage:
            storage.save_run(run_meta)
        raise
    finally:
        if storage and hasattr(storage, 'close'):
            storage.close()


def initialize_storage(cfg: TrendMinerConfig) -> Optional[object]:
    """初始化儲存後端（fail fast，不 fallback）"""
    if cfg.storage.mode == "postgres":
        dsn = cfg.storage.postgres_dsn
        if not dsn:
            raise ValueError("Postgres mode requires storage.postgres_dsn")
        
        # 連線失敗直接拋出異常（不 fallback）
        logger.info(f"Initializing Postgres storage...")
        return PostgresStore(dsn, auto_init_schema=True)
    
    elif cfg.storage.mode == "file":
        logger.info("Using file storage backend")
        return FileStore()
    
    else:
        raise ValueError(f"Unsupported storage mode: {cfg.storage.mode}")


def write_output_files(output_dir: Path, run_id: str, items, topics):
    """寫入輸出檔案"""
    import json
    
    # topics.json
    topics_file = output_dir / "topics.json"
    with open(topics_file, 'w', encoding='utf-8') as f:
        topics_data = [topic.model_dump() for topic in topics]
        json.dump(topics_data, f, indent=2, ensure_ascii=False, default=str)
    
    # items.jsonl
    items_file = output_dir / "items.jsonl"
    with open(items_file, 'w', encoding='utf-8') as f:
        for item in items:
            json_line = json.dumps(item.model_dump(), ensure_ascii=False, default=str)
            f.write(json_line + '\n')
    
    logger.info(f"Written: {topics_file}, {items_file}")


if __name__ == "__main__":
    cli()
