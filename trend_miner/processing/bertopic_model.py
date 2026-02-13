"""
BERTopic clustering with deterministic configuration

核心分群邏輯，使用 BERTopic + Multilingual Embeddings
"""

from typing import List, Tuple, Dict, Optional
import numpy as np
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from umap import UMAP
from hdbscan import HDBSCAN
import logging

from trend_miner.models import ItemRecord
from trend_miner.config import BERTopicConfig

logger = logging.getLogger(__name__)


def set_random_seeds(seed: int):
    """設定 random seeds (確保 determinism)"""
    np.random.seed(seed)
    import random
    random.seed(seed)


def create_bertopic_model(
    config: BERTopicConfig,
    global_random_seed: int
) -> BERTopic:
    """
    建立 BERTopic model
    
    Args:
        config: BERTopic 設定
        global_random_seed: 全局 random seed
    
    Returns:
        BERTopic instance
    """
    set_random_seeds(global_random_seed)
    
    # Embedding model
    embedding_model = SentenceTransformer(config.embedding_model)
    
    # UMAP model (使用固定 random_state)
    umap_model = UMAP(
        n_neighbors=config.umap_n_neighbors,
        n_components=config.umap_n_components,
        min_dist=config.umap_min_dist,
        metric=config.umap_metric,
        random_state=config.umap_random_state
    )
    
    # HDBSCAN model
    hdbscan_min_cluster_size = config.hdbscan_min_cluster_size or config.min_topic_size
    hdbscan_min_samples = config.hdbscan_min_samples or (config.min_topic_size // 2)
    
    hdbscan_model = HDBSCAN(
        min_cluster_size=hdbscan_min_cluster_size,
        min_samples=hdbscan_min_samples,
        metric=config.hdbscan_metric,
        prediction_data=True
    )
    
    # BERTopic
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        nr_topics=config.nr_topics,
        top_n_words=config.top_n_words,
        calculate_probabilities=config.calculate_probabilities,
        verbose=False
    )
    
    logger.info(f"Created BERTopic model with embedding: {config.embedding_model}")
    logger.info(f"UMAP: n_neighbors={config.umap_n_neighbors}, random_state={config.umap_random_state}")
    logger.info(f"HDBSCAN: min_cluster_size={hdbscan_min_cluster_size}, min_samples={hdbscan_min_samples}")
    
    return topic_model


def cluster_items(
    items: List[ItemRecord],
    config: BERTopicConfig,
    global_random_seed: int,
    min_items_to_cluster: int = 30,
    noise_handling: str = "drop"
) -> Tuple[List[ItemRecord], Dict]:
    """
    使用 BERTopic 對 items 進行分群
    
    Args:
        items: ItemRecords
        config: BERTopic 設定
        global_random_seed: 全局 random seed
        min_items_to_cluster: 最少聚類項目數
        noise_handling: Noise 處理方式 ("drop" | "keep")
    
    Returns:
        (更新 topic_id 的 items, 統計資訊)
    """
    stats = {
        'n_items': len(items),
        'n_topics': 0,
        'noise_count': 0,
        'noise_ratio': 0.0,
        'degraded': False
    }
    
    # 檢查是否需要降級 (items 太少)
    if len(items) < min_items_to_cluster:
        logger.warning(f"Items count ({len(items)}) < min_items_to_cluster ({min_items_to_cluster}), " +
                      "degrading to single topic or all noise")
        
        # 降級策略：全部設為 topic 0 (單一主題)
        for item in items:
            item.topic_id = 0
        
        stats['n_topics'] = 1
        stats['degraded'] = True
        return items, stats
    
    # 準備文本 (title + summary)
    texts = [f"{item.title} {item.summary}" for item in items]
    
    # 建立並訓練 BERTopic model
    logger.info(f"Training BERTopic on {len(texts)} documents...")
    topic_model = create_bertopic_model(config, global_random_seed)
    
    topics, _ = topic_model.fit_transform(texts)
    
    # 更新 items 的 topic_id
    for item, topic_id in zip(items, topics):
        item.topic_id = int(topic_id)
    
    # 統計
    unique_topics = set(topics)
    n_noise = sum(1 for t in topics if t == -1)
    n_topics = len(unique_topics) - (1 if -1 in unique_topics else 0)
    
    stats['n_topics'] = n_topics
    stats['noise_count'] = n_noise
    stats['noise_ratio'] = n_noise / len(items) if len(items) > 0 else 0
    
    logger.info(f"BERTopic clustering complete: {n_topics} topics, {n_noise} noise items ({stats['noise_ratio']:.1%})")
    
    # Noise 處理
    if noise_handling == "drop":
        items_filtered = [item for item in items if item.topic_id != -1]
        logger.info(f"Noise handling: dropped {n_noise} noise items")
        return items_filtered, stats
    else:
        # Keep noise
        return items, stats


def get_topic_keywords(
    items: List[ItemRecord],
    config: BERTopicConfig,
    global_random_seed: int,
    top_n: int = 15
) -> Dict[int, List[str]]:
    """
    取得每個 topic 的關鍵字
    
    Args:
        items: 已分群的 ItemRecords
        config: BERTopic 設定
        global_random_seed: 全局 random seed
        top_n: 每個 topic 的關鍵字數
    
    Returns:
        {topic_id: [keywords]}
    """
    # 重建 BERTopic model (需要重新 fit)
    texts = [f"{item.title} {item.summary}" for item in items]
    topic_model = create_bertopic_model(config, global_random_seed)
    topics = [item.topic_id for item in items]
    
    # Fit model with existing topic assignments 
    # (這會產生 topic representations)
    topic_model.fit(texts, y=topics)
    
    # 提取關鍵字
    topic_keywords = {}
    
    for topic_id in set(topics):
        if topic_id == -1:
            continue
        
        try:
            # 取得 topic words
            words = topic_model.get_topic(topic_id)
            if words:
                topic_keywords[topic_id] = [word for word, score in words[:top_n]]
        except:
            topic_keywords[topic_id] = []
    
    return topic_keywords
