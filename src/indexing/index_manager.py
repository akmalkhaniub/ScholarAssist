"""
ScholarAssist — OpenSearch Index Manager

Handles index lifecycle operations:
- Creating versioned indices
- Managing aliases for zero-downtime reindexing
- Cleaning up old indices
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from opensearchpy import OpenSearch, exceptions

from src.config.settings import OpenSearchSettings
from src.indexing.mappings import get_index_body

logger = logging.getLogger(__name__)


class IndexManager:
    """
    Manages OpenSearch indices and aliases for zero-downtime updates.
    """
    def __init__(self, settings: OpenSearchSettings) -> None:
        self.settings = settings
        
        # Configure client
        kwargs = {
            "hosts": [{"host": self.settings.url.replace("http://", "").replace("https://", "").split(":")[0], 
                       "port": int(self.settings.url.split(":")[-1]) if ":" in self.settings.url else 9200}],
            "use_ssl": self.settings.use_ssl,
            "verify_certs": self.settings.verify_certs,
            "timeout": self.settings.timeout,
        }
        
        if self.settings.username and self.settings.password:
            kwargs["http_auth"] = (self.settings.username, self.settings.password)
            
        self.client = OpenSearch(**kwargs)

    def create_new_index_version(self) -> str:
        """
        Creates a new timestamp-versioned index.
        Returns the name of the created index.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        index_name = f"{self.settings.index_prefix}{timestamp}"
        
        body = get_index_body(
            number_of_shards=self.settings.number_of_shards,
            number_of_replicas=self.settings.number_of_replicas
        )
        
        # Disable refresh during bulk indexing for performance
        body["settings"]["index"]["refresh_interval"] = "-1"
        
        logger.info(f"Creating new index version: {index_name}")
        self.client.indices.create(index=index_name, body=body)
        
        return index_name

    def finish_indexing(self, index_name: str) -> None:
        """
        Restores standard settings (refresh interval) after bulk indexing.
        """
        logger.info(f"Restoring standard settings for {index_name}")
        self.client.indices.put_settings(
            index=index_name,
            body={
                "index": {
                    "refresh_interval": "30s"
                }
            }
        )
        # Force a refresh to make documents searchable immediately
        self.client.indices.refresh(index=index_name)

    def switch_alias(self, new_index: str) -> None:
        """
        Atomically switches the main alias to point to the new index.
        Removes the alias from any old indices.
        """
        alias = self.settings.index_alias
        
        # Find existing indices mapped to the alias
        old_indices = []
        try:
            aliases = self.client.indices.get_alias(name=alias)
            old_indices = list(aliases.keys())
        except exceptions.NotFoundError:
            pass # Alias doesn't exist yet
            
        actions = []
        
        # Remove alias from old indices
        for old_idx in old_indices:
            if old_idx != new_index:
                actions.append({"remove": {"index": old_idx, "alias": alias}})
                
        # Add alias to new index
        actions.append({"add": {"index": new_index, "alias": alias}})
        
        logger.info(f"Switching alias '{alias}' to index '{new_index}'")
        self.client.indices.update_aliases(body={"actions": actions})

    def cleanup_old_indices(self, keep_latest: int = 2) -> None:
        """
        Deletes old index versions to save storage, keeping the N most recent.
        Does NOT delete any index currently pointed to by the active alias.
        """
        # Get all indices matching the prefix
        all_indices = list(self.client.indices.get(index=f"{self.settings.index_prefix}*").keys())
        all_indices.sort() # Sorts by timestamp suffix
        
        # Get the currently active index
        active_indices = []
        try:
            aliases = self.client.indices.get_alias(name=self.settings.index_alias)
            active_indices = list(aliases.keys())
        except exceptions.NotFoundError:
            pass
            
        indices_to_delete = [idx for idx in all_indices[:-keep_latest] if idx not in active_indices]
        
        for idx in indices_to_delete:
            logger.info(f"Deleting old index: {idx}")
            self.client.indices.delete(index=idx)
