"""
File-based storage backend

所有 run 的資料都儲存在本地檔案系統。
"""

import os
import json
from typing import List
from pathlib import Path
import logging

from trend_miner.models import ItemRecord, TopicRecord, RunMetadata

logger = logging.getLogger(__name__)


class FileStore:
    """檔案儲存後端"""
    
    def __init__(self, base_dir: str = "memory"):
        """
        初始化 FileStore
        
        Args:
            base_dir: 基礎目錄
        """
        self.base_dir = Path(base_dir)
        self.runs_dir = self.base_dir / "runs"
        self.items_dir = self.base_dir / "items"
        self.topics_dir = self.base_dir / "topics"
        
        # 建立目錄
        for dir_path in [self.runs_dir, self.items_dir, self.topics_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"FileStore initialized at {self.base_dir}")
    
    def save_run(self, run_meta: RunMetadata) -> None:
        """寫入 run metadata"""
        file_path = self.runs_dir / f"{run_meta.run_id}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(run_meta.model_dump(), f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Written run metadata: {file_path}")
    
    def save_items(self, items: List[ItemRecord]) -> None:
        """寫入 items (JSONL格式)"""
        if not items:
            return
        
        run_id = items[0].run_id
        file_path = self.items_dir / f"{run_id}.jsonl"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for item in items:
                json_line = json.dumps(item.model_dump(), ensure_ascii=False, default=str)
                f.write(json_line + '\n')
        
        logger.info(f"Written {len(items)} items: {file_path}")
    
    def save_topics(self, topics: List[TopicRecord]) -> None:
        """寫入 topics (JSON格式)"""
        if not topics:
            return
        
        run_id = topics[0].run_id
        file_path = self.topics_dir / f"{run_id}.json"
        
        topics_data = [topic.model_dump() for topic in topics]
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(topics_data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Written {len(topics)} topics: {file_path}")
    
    def read_items(self, run_id: str) -> List[ItemRecord]:
        """讀取 items"""
        file_path = self.items_dir / f"{run_id}.jsonl"
        
        if not file_path.exists():
            return []
        
        items = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                item_data = json.loads(line)
                items.append(ItemRecord(**item_data))
        
        return items
    
    def read_topics(self, run_id: str) -> List[TopicRecord]:
        """讀取 topics"""
        file_path = self.topics_dir / f"{run_id}.json"
        
        if not file_path.exists():
            return []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            topics_data = json.load(f)
        
        topics = [TopicRecord(**topic_data) for topic_data in topics_data]
        return topics
