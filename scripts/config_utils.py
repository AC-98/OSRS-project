#!/usr/bin/env python3
"""
OSRS Signals - Configuration Utilities

Shared utilities for reading item configurations and selected items.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def load_selected_items(
    config_path: str = "config/items_selected.json",
    fallback_items: Optional[List[int]] = None
) -> List[int]:
    """
    Load selected item IDs from configuration file.
    
    Args:
        config_path: Path to items_selected.json file
        fallback_items: Fallback list if config file is missing
        
    Returns:
        List of item IDs to use
    """
    if fallback_items is None:
        fallback_items = [4151, 561, 5616]  # Default demo items
    
    try:
        config_file = Path(config_path)
        
        if not config_file.exists():
            logger.warning(f"Config file {config_path} not found, using fallback items: {fallback_items}")
            return fallback_items
        
        with open(config_file, 'r') as f:
            data = json.load(f)
        
        # Extract item IDs from the config
        items = data.get("items", [])
        item_ids = [item["id"] for item in items if isinstance(item, dict) and "id" in item]
        
        if not item_ids:
            logger.warning(f"No items found in {config_path}, using fallback items: {fallback_items}")
            return fallback_items
        
        logger.info(f"Loaded {len(item_ids)} items from {config_path}: {item_ids}")
        return item_ids
        
    except Exception as e:
        logger.error(f"Failed to load items from {config_path}: {e}")
        logger.info(f"Using fallback items: {fallback_items}")
        return fallback_items


def resolve_items_argument(items_arg: str) -> List[int]:
    """
    Resolve items argument that can be:
    - A list of integers: [4151, 561, 5616]
    - A config file reference: @config/items_selected.json
    - Comma-separated string: "4151,561,5616"
    
    Args:
        items_arg: Items argument to resolve
        
    Returns:
        List of item IDs
    """
    try:
        # Handle config file reference
        if items_arg.startswith('@'):
            config_path = items_arg[1:]  # Remove @ prefix
            return load_selected_items(config_path)
        
        # Handle comma-separated string
        if ',' in items_arg:
            return [int(x.strip()) for x in items_arg.split(',')]
        
        # Handle single item
        return [int(items_arg)]
        
    except Exception as e:
        logger.error(f"Failed to resolve items argument '{items_arg}': {e}")
        return [4151, 561, 5616]  # Fallback
