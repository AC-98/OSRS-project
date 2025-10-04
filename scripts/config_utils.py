#!/usr/bin/env python3
"""
OSRS Signals - Configuration Utilities

Shared utilities for reading item configurations and selected items.
"""

import json
import logging
import yaml
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd

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


def resolve_names_to_ids_from_mapping(
    names: List[str],
    mapping_df: pd.DataFrame
) -> List[int]:
    """
    Resolve item names to IDs using the mapping DataFrame.
    
    Performs case-insensitive exact match. Logs warnings for unresolved names.
    
    Args:
        names: List of item names to resolve
        mapping_df: DataFrame with 'id' and 'name' columns
        
    Returns:
        List of resolved item IDs
    """
    if not names:
        return []
    
    resolved_ids = []
    
    # Create lowercase mapping for case-insensitive lookup
    mapping_df['name_lower'] = mapping_df['name'].str.lower()
    
    for name in names:
        name_lower = name.lower()
        matches = mapping_df[mapping_df['name_lower'] == name_lower]
        
        if len(matches) == 0:
            logger.warning(f"No item found matching name '{name}'")
        elif len(matches) == 1:
            item_id = int(matches.iloc[0]['id'])
            resolved_ids.append(item_id)
            logger.info(f"Resolved '{name}' → {item_id} ({matches.iloc[0]['name']})")
        else:
            # Multiple exact matches (shouldn't happen, but handle it)
            item_id = int(matches.iloc[0]['id'])
            resolved_ids.append(item_id)
            logger.warning(
                f"Multiple items found for '{name}', using first: "
                f"{item_id} ({matches.iloc[0]['name']})"
            )
    
    logger.info(f"Resolved {len(resolved_ids)} out of {len(names)} names")
    return resolved_ids


def compute_auto_discover_candidates(
    mapping_df: pd.DataFrame,
    min_limit: int,
    members_only: Optional[bool],
    top_k: int
) -> List[int]:
    """
    Compute auto-discovery candidates based on GE buy limits.
    
    Uses the 'limit' column as a proxy for item liquidity. Higher limits
    typically indicate more liquid items.
    
    Args:
        mapping_df: DataFrame with item mapping (id, limit, members columns)
        min_limit: Minimum GE buy limit threshold
        members_only: Filter by members status (None = both, True = members, False = F2P)
        top_k: Number of top items to return
        
    Returns:
        List of item IDs sorted by limit (descending)
    """
    df = mapping_df.copy()
    
    # Filter by minimum limit
    df = df[df['limit'] >= min_limit]
    
    # Filter by members status if specified
    if members_only is not None:
        df = df[df['members'] == members_only]
    
    # Sort by limit descending and take top_k
    df = df.sort_values('limit', ascending=False).head(top_k)
    
    item_ids = df['id'].tolist()
    
    logger.info(
        f"Auto-discover: found {len(item_ids)} items "
        f"(min_limit={min_limit}, members_only={members_only}, top_k={top_k})"
    )
    
    if item_ids:
        logger.info(f"Top auto-discovered items: {item_ids[:5]}")
    
    return item_ids


def load_ingestion_config(config_path: str = "config/items.yaml") -> Dict[str, Any]:
    """
    Load ingestion configuration from YAML file.
    
    Args:
        config_path: Path to items.yaml file
        
    Returns:
        Dictionary with ingest_targets and runtime_caps sections
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        ingest_targets = config.get('ingest_targets', {})
        runtime_caps = config.get('runtime_caps', {})
        
        # Set defaults if missing
        if not ingest_targets:
            ingest_targets = {
                'include_ids': [],
                'include_names': [],
                'exclude_ids': [],
                'exclude_names': [],
                'auto_discover': {
                    'enabled': False,
                    'top_k': 10,
                    'min_limit': 1000,
                    'members_only': None
                }
            }
        
        if not runtime_caps:
            runtime_caps = {
                'max_items_per_run': 25,
                'request_sleep_ms': 450
            }
        
        return {
            'ingest_targets': ingest_targets,
            'runtime_caps': runtime_caps
        }
        
    except Exception as e:
        logger.error(f"Failed to load ingestion config from {config_path}: {e}")
        # Return safe defaults
        return {
            'ingest_targets': {
                'include_ids': [],
                'include_names': [],
                'exclude_ids': [],
                'exclude_names': [],
                'auto_discover': {'enabled': False, 'top_k': 10, 'min_limit': 1000, 'members_only': None}
            },
            'runtime_caps': {
                'max_items_per_run': 25,
                'request_sleep_ms': 450
            }
        }
