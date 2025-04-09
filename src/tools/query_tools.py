"""
Tools for querying and retrieving records from Opendatasoft datasets.
"""
from typing import List, Dict, Any, Optional
import json
from ..ods_api import OdsApiClient

async def get_dataset_records(
    api_client: OdsApiClient,
    dataset_id: str,
    limit: int = 10,
    offset: int = 0,
    select: Optional[str] = None,
    where: Optional[str] = None,
    order_by: Optional[str] = None
) -> str:
    """
    Get records from a dataset with optional filtering and sorting.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        limit: Maximum number of records to return
        offset: Offset for pagination
        select: ODSQL select clause
        where: ODSQL where clause
        order_by: ODSQL order by clause
        
    Returns:
        Formatted string with dataset records
    """
    try:
        results = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=select,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        return f"Error retrieving dataset records: {str(e)}"
    
    if not results or "results" not in results or not results["results"]:
        return f"No records found for dataset '{dataset_id}' with the specified criteria."
    
    records = results["results"]
    total_count = results.get("total_count", 0)
    
    # Get dataset info to include in the output
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    except:
        dataset_title = "Unknown Dataset"
    
    output = [
        f"Records from dataset: {dataset_title} (ID: {dataset_id})",
        f"Showing {len(records)} of {total_count} total records (offset: {offset})"
    ]
    
    # If there are no records, return early
    if not records:
        output.append("\nNo records found.")
        return "\n".join(output)
    
    # Build a table-like representation of the records
    record_keys = list(records[0].keys())
    
    # If there are too many fields, summarize the first few records differently
    if len(record_keys) > 10:
        output.append(f"\nFound {len(record_keys)} fields in the records. Here's a summary of the first {len(records)} records:")
        
        for i, record in enumerate(records, 1):
            output.append(f"\nRecord {i}:")
            for key, value in record.items():
                # Format the value for display
                if isinstance(value, dict) or isinstance(value, list):
                    value = json.dumps(value, ensure_ascii=False)
                output.append(f"  {key}: {value}")
    else:
        # Format as a table with headers when fewer fields
        headers = [key for key in record_keys]
        output.append("\n| " + " | ".join(headers) + " |")
        output.append("| " + " | ".join(["---"] * len(headers)) + " |")
        
        for record in records:
            values = []
            for key in record_keys:
                value = record.get(key, "")
                # Format the value for display
                if isinstance(value, dict) or isinstance(value, list):
                    value = json.dumps(value, ensure_ascii=False)
                values.append(str(value))
            output.append("| " + " | ".join(values) + " |")
    
    # Add a note about ODSQL syntax if any parameters were used
    if any([select, where, order_by]):
        output.append("\nNote: This query used ODSQL syntax:")
        if select:
            output.append(f"- SELECT: {select}")
        if where:
            output.append(f"- WHERE: {where}")
        if order_by:
            output.append(f"- ORDER BY: {order_by}")
    
    return "\n".join(output)

async def get_dataset_aggregates(
    api_client: OdsApiClient,
    dataset_id: str,
    select: str,
    group_by: Optional[str] = None,
    where: Optional[str] = None,
    limit: int = 100
) -> str:
    """
    Get aggregated data from a dataset using ODSQL aggregation functions.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        select: ODSQL select clause with aggregation functions
        group_by: ODSQL group by clause
        where: ODSQL where clause
        limit: Maximum number of results
        
    Returns:
        Formatted string with aggregation results
    """
    try:
        results = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=select,
            group_by=group_by,
            where=where,
            limit=limit
        )
    except Exception as e:
        return f"Error performing aggregation: {str(e)}"
    
    if not results or "results" not in results or not results["results"]:
        return f"No aggregation results found for dataset '{dataset_id}' with the specified criteria."
    
    records = results["results"]
    
    # Get dataset info to include in the output
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    except:
        dataset_title = "Unknown Dataset"
    
    output = [
        f"Aggregation results for dataset: {dataset_title} (ID: {dataset_id})",
        f"Query: SELECT {select}" + (f" GROUP BY {group_by}" if group_by else "") + (f" WHERE {where}" if where else ""),
        f"Results: {len(records)} rows"
    ]
    
    # If there are no records, return early
    if not records:
        output.append("\nNo results found.")
        return "\n".join(output)
    
    # Build a table representation of the results
    record_keys = list(records[0].keys())
    
    # Format as a table with headers
    headers = [key for key in record_keys]
    output.append("\n| " + " | ".join(headers) + " |")
    output.append("| " + " | ".join(["---"] * len(headers)) + " |")
    
    for record in records:
        values = []
        for key in record_keys:
            value = record.get(key, "")
            # Format the value for display
            if isinstance(value, dict) or isinstance(value, list):
                value = json.dumps(value, ensure_ascii=False)
            values.append(str(value))
        output.append("| " + " | ".join(values) + " |")
    
    return "\n".join(output)

async def facet_analysis(
    api_client: OdsApiClient,
    dataset_id: str,
    facets: List[str],
    where: Optional[str] = None
) -> str:
    """
    Analyze facet values distribution for a dataset.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        facets: List of facet field names
        where: ODSQL where clause to filter results
        
    Returns:
        Formatted string with facet analysis
    """
    try:
        results = await api_client.get_dataset_facets(
            dataset_id=dataset_id,
            facet=facets,
            where=where
        )
    except Exception as e:
        return f"Error retrieving facets: {str(e)}"
    
    if not results or "facets" not in results or not results["facets"]:
        return f"No facet data found for dataset '{dataset_id}' with the specified criteria."
    
    # Get dataset info to include in the output
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    except:
        dataset_title = "Unknown Dataset"
    
    output = [
        f"Facet analysis for dataset: {dataset_title} (ID: {dataset_id})",
        f"Analyzing facets: {', '.join(facets)}" + (f" WHERE {where}" if where else "")
    ]
    
    # Process each facet
    for facet_data in results["facets"]:
        facet_name = facet_data.get("name", "Unknown")
        facet_values = facet_data.get("facets", [])
        
        output.append(f"\nFacet: {facet_name} ({len(facet_values)} values)")
        
        if not facet_values:
            output.append("  No values found for this facet.")
            continue
        
        # Sort by count (descending)
        sorted_values = sorted(facet_values, key=lambda x: x.get("count", 0), reverse=True)
        
        # Build a table for the values
        output.append("\n| Value | Count | State |")
        output.append("| --- | --- | --- |")
        
        for value in sorted_values[:20]:  # Limit to top 20 values
            value_name = value.get("name", "N/A")
            count = value.get("count", 0)
            state = value.get("state", "N/A")
            
            output.append(f"| {value_name} | {count} | {state} |")
        
        if len(sorted_values) > 20:
            output.append(f"\n(Showing top 20 of {len(sorted_values)} values)")
    
    return "\n".join(output)

async def search_dataset_records(
    api_client: OdsApiClient,
    dataset_id: str,
    query: str,
    limit: int = 10
) -> str:
    """
    Search for specific records within a dataset.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        query: Search query
        limit: Maximum number of results
        
    Returns:
        Formatted string with search results
    """
    try:
        results = await api_client.search_records(
            dataset_id=dataset_id,
            query=query,
            limit=limit
        )
    except Exception as e:
        return f"Error searching dataset records: {str(e)}"
    
    if not results or "results" not in results or not results["results"]:
        return f"No records found matching '{query}' in dataset '{dataset_id}'."
    
    records = results["results"]
    total_count = results.get("total_count", 0)
    
    # Get dataset info to include in the output
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    except:
        dataset_title = "Unknown Dataset"
    
    output = [
        f"Search results for '{query}' in dataset: {dataset_title} (ID: {dataset_id})",
        f"Found {total_count} matching records. Showing first {len(records)}:"
    ]
    
    # If there are no records, return early
    if not records:
        output.append("\nNo matching records found.")
        return "\n".join(output)
    
    # Process each record
    for i, record in enumerate(records, 1):
        output.append(f"\nRecord {i}:")
        for key, value in record.items():
            # Format the value for display
            if isinstance(value, dict) or isinstance(value, list):
                value = json.dumps(value, ensure_ascii=False)
            output.append(f"  {key}: {value}")
    
    return "\n".join(output)

async def get_export_url(
    api_client: OdsApiClient,
    dataset_id: str,
    export_format: str = "csv",
    select: Optional[str] = None,
    where: Optional[str] = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """
    Get a URL for exporting dataset records in various formats.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        export_format: Export format (csv, json, geojson, etc.)
        select: ODSQL select clause
        where: ODSQL where clause
        group_by: ODSQL group by clause
        order_by: ODSQL order by clause
        limit: Maximum number of results
        
    Returns:
        Export URL for the specified format
    """
    try:
        export_url = await api_client.export_records(
            dataset_id=dataset_id,
            export_format=export_format,
            select=select,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit
        )
    except Exception as e:
        return f"Error generating export URL: {str(e)}"
    
    # Get dataset info to include in the output
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    except:
        dataset_title = "Unknown Dataset"
    
    output = [
        f"Export URL for dataset: {dataset_title} (ID: {dataset_id})",
        f"Format: {export_format.upper()}"
    ]
    
    # Add query parameters if specified
    query_params = []
    if select:
        query_params.append(f"SELECT: {select}")
    if where:
        query_params.append(f"WHERE: {where}")
    if group_by:
        query_params.append(f"GROUP BY: {group_by}")
    if order_by:
        query_params.append(f"ORDER BY: {order_by}")
    if limit:
        query_params.append(f"LIMIT: {limit}")
    
    if query_params:
        output.append(f"Query parameters: {', '.join(query_params)}")
    
    output.append(f"\nExport URL: {export_url}")
    output.append("\nNote: This URL can be used to download the dataset in the specified format.")
    
    return "\n".join(output)