"""
Tools for discovering and exploring datasets in the Opendatasoft catalog.
"""
from typing import List, Dict, Any, Optional
import json
import os
import sys

from src.ods_api import OdsApiClient

async def search_datasets(
    api_client: OdsApiClient,
    query: str,
    limit: int = 10
) -> str:
    """
    Search for datasets by keyword.
    
    Args:
        api_client: OdsApiClient instance
        query: Search query
        limit: Maximum number of results
        
    Returns:
        Formatted string with search results
    """
    results = await api_client.search_datasets(query, limit=limit)
    
    if not results or "results" not in results or not results["results"]:
        return "No datasets found matching your query."
    
    total_count = results.get("total_count", 0)
    datasets = results["results"]
    
    output = [f"Found {total_count} datasets matching '{query}'. Here are the first {len(datasets)} results:"]
    
    for i, dataset in enumerate(datasets, 1):
        dataset_id = dataset.get("dataset_id", "N/A")
        title = dataset.get("metas", {}).get("default", {}).get("title", "Untitled Dataset")
        publisher = dataset.get("metas", {}).get("default", {}).get("publisher", "Unknown Publisher")
        description = dataset.get("metas", {}).get("default", {}).get("description", "No description available.")
        
        # Clean up description - remove HTML tags
        description = description.replace("<p>", "").replace("</p>", " ").replace("<br>", " ")
        
        # Truncate long descriptions
        if len(description) > 300:
            description = description[:297] + "..."
        
        output.append(f"\n{i}. {title} (ID: {dataset_id})")
        output.append(f"   Publisher: {publisher}")
        output.append(f"   Description: {description}")
    
    return "\n".join(output)

async def get_dataset_info(
    api_client: OdsApiClient,
    dataset_id: str
) -> str:
    """
    Get detailed information about a specific dataset.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        
    Returns:
        Formatted string with dataset information
    """
    try:
        dataset = await api_client.get_dataset(dataset_id)
    except Exception as e:
        return f"Error retrieving dataset: {str(e)}"
    
    if not dataset:
        return f"Dataset with ID '{dataset_id}' not found."
    
    # Extract metadata
    metas = dataset.get("metas", {}).get("default", {})
    title = metas.get("title", "Untitled Dataset")
    publisher = metas.get("publisher", "Unknown Publisher")
    description = metas.get("description", "No description available.")
    records_count = metas.get("records_count", "Unknown")
    
    # Clean up description - remove HTML tags
    description = description.replace("<p>", "").replace("</p>", " ").replace("<br>", " ")
    
    # Extract fields information
    fields = dataset.get("fields", [])
    field_info = []
    for field in fields:
        name = field.get("name", "Unnamed")
        label = field.get("label", name)
        field_type = field.get("type", "Unknown")
        field_desc = field.get("description", "")
        
        field_info.append(f"  - {label} ({name}): {field_type}{' - ' + field_desc if field_desc else ''}")
    
    # Build output
    output = [
        f"Dataset: {title} (ID: {dataset_id})",
        f"Publisher: {publisher}",
        f"Record Count: {records_count}",
        f"\nDescription:",
        f"{description}",
        f"\nFields ({len(fields)}):"
    ]
    
    output.extend(field_info)
    
    return "\n".join(output)

async def list_datasets_by_publisher(
    api_client: OdsApiClient,
    publisher: str,
    limit: int = 10
) -> str:
    """
    List datasets from a specific publisher.
    
    Args:
        api_client: OdsApiClient instance
        publisher: Publisher name
        limit: Maximum number of results
        
    Returns:
        Formatted string with datasets from the specified publisher
    """
    results = await api_client.list_datasets(publisher=publisher, limit=limit)
    
    if not results or "results" not in results or not results["results"]:
        return f"No datasets found from publisher: {publisher}."
    
    total_count = results.get("total_count", 0)
    datasets = results["results"]
    
    output = [f"Found {total_count} datasets from publisher '{publisher}'. Here are the first {len(datasets)} results:"]
    
    for i, dataset in enumerate(datasets, 1):
        dataset_id = dataset.get("dataset_id", "N/A")
        title = dataset.get("metas", {}).get("default", {}).get("title", "Untitled Dataset")
        
        # Get record count
        records_count = dataset.get("metas", {}).get("default", {}).get("records_count", "Unknown")
        
        # Get theme if available
        theme = dataset.get("metas", {}).get("default", {}).get("theme", [""])[0]
        theme_info = f" | Theme: {theme}" if theme else ""
        
        output.append(f"\n{i}. {title} (ID: {dataset_id})")
        output.append(f"   Records: {records_count}{theme_info}")
    
    return "\n".join(output)

async def list_dataset_fields(
    api_client: OdsApiClient,
    dataset_id: str
) -> str:
    """
    List all fields in a dataset with their types and descriptions.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        
    Returns:
        Formatted string with dataset fields
    """
    try:
        dataset = await api_client.get_dataset(dataset_id)
    except Exception as e:
        return f"Error retrieving dataset: {str(e)}"
    
    if not dataset:
        return f"Dataset with ID '{dataset_id}' not found."
    
    fields = dataset.get("fields", [])
    if not fields:
        return f"No fields found for dataset '{dataset_id}'."
    
    title = dataset.get("metas", {}).get("default", {}).get("title", "Untitled Dataset")
    
    output = [f"Fields for dataset: {title} (ID: {dataset_id})"]
    
    for i, field in enumerate(fields, 1):
        name = field.get("name", "Unnamed")
        label = field.get("label", name)
        field_type = field.get("type", "Unknown")
        description = field.get("description", "No description available")
        
        output.append(f"\n{i}. {label} ({name})")
        output.append(f"   Type: {field_type}")
        output.append(f"   Description: {description}")
        
        # Add annotations information if available
        annotations = field.get("annotations", {})
        if annotations:
            annotation_str = ", ".join(f"{k}: {v}" for k, v in annotations.items())
            output.append(f"   Annotations: {annotation_str}")
    
    return "\n".join(output)