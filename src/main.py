"""
Opendatasoft MCP Server

A Model Context Protocol server that provides tools for interacting with the OpenDatasoft
Explore API, enabling AI assistants like Claude to search, query, and analyze open datasets.
"""
import asyncio
import os
import sys
from typing import Dict, Any, Optional, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mcp.server.fastmcp import FastMCP

from src.ods_api import OdsApiClient
from src.tools import catalog_tools, query_tools, analysis_tools

# Get API configuration from environment variables or use defaults
ODS_BASE_URL = os.environ.get("ODS_BASE_URL", "https://documentation-resources.opendatasoft.com")
ODS_API_KEY = os.environ.get("ODS_API_KEY", None)

# Initialize FastMCP server
mcp = FastMCP("opendatasoft")

# Initialize the ODS API client
api_client = OdsApiClient(ODS_BASE_URL, ODS_API_KEY)

"""
Catalog Tools - Dataset discovery and exploration
"""

@mcp.tool()
async def search_datasets(query: str, limit: int = 10) -> str:
    """Search for datasets by keyword.
    
    Args:
        query: Search query to find datasets
        limit: Maximum number of datasets to return (default: 10)
    """
    return await catalog_tools.search_datasets(api_client, query, limit)

@mcp.tool()
async def get_dataset_info(dataset_id: str) -> str:
    """Get detailed information about a specific dataset.
    
    Args:
        dataset_id: Unique identifier for the dataset
    """
    return await catalog_tools.get_dataset_info(api_client, dataset_id)

@mcp.tool()
async def list_datasets_by_publisher(publisher: str, limit: int = 10) -> str:
    """List datasets from a specific publisher.
    
    Args:
        publisher: Name of the publisher
        limit: Maximum number of datasets to return (default: 10)
    """
    return await catalog_tools.list_datasets_by_publisher(api_client, publisher, limit)

@mcp.tool()
async def list_dataset_fields(dataset_id: str) -> str:
    """List all fields in a dataset with their types and descriptions.
    
    Args:
        dataset_id: Unique identifier for the dataset
    """
    return await catalog_tools.list_dataset_fields(api_client, dataset_id)

"""
Query Tools - Data retrieval and querying
"""

@mcp.tool()
async def get_dataset_records(
    dataset_id: str,
    limit: int = 10,
    offset: int = 0,
    select: Optional[str] = None,
    where: Optional[str] = None,
    order_by: Optional[str] = None
) -> str:
    """Get records from a dataset with optional filtering and sorting.
    
    Args:
        dataset_id: Unique identifier for the dataset
        limit: Maximum number of records to return (default: 10)
        offset: Number of records to skip (for pagination)
        select: ODSQL select clause to choose specific fields
        where: ODSQL where clause to filter records
        order_by: ODSQL order by clause to sort records
    """
    return await query_tools.get_dataset_records(
        api_client, dataset_id, limit, offset, select, where, order_by
    )

@mcp.tool()
async def get_dataset_aggregates(
    dataset_id: str,
    select: str,
    group_by: Optional[str] = None,
    where: Optional[str] = None,
    limit: int = 100
) -> str:
    """Get aggregated data from a dataset using ODSQL aggregation functions.
    
    Args:
        dataset_id: Unique identifier for the dataset
        select: ODSQL select clause with aggregation functions (count, sum, avg, etc.)
        group_by: ODSQL group by clause to aggregate by field values
        where: ODSQL where clause to filter records
        limit: Maximum number of results (default: 100)
    """
    return await query_tools.get_dataset_aggregates(
        api_client, dataset_id, select, group_by, where, limit
    )

@mcp.tool()
async def facet_analysis(
    dataset_id: str,
    facets: str,
    where: Optional[str] = None
) -> str:
    """Analyze facet values distribution for a dataset.
    
    Args:
        dataset_id: Unique identifier for the dataset
        facets: Comma-separated list of field names to use as facets
        where: ODSQL where clause to filter records
    """
    facet_list = [f.strip() for f in facets.split(",")]
    return await query_tools.facet_analysis(api_client, dataset_id, facet_list, where)

@mcp.tool()
async def search_dataset_records(
    dataset_id: str,
    query: str,
    limit: int = 10
) -> str:
    """Search for specific records within a dataset.
    
    Args:
        dataset_id: Unique identifier for the dataset
        query: Search query to find records
        limit: Maximum number of records to return (default: 10)
    """
    return await query_tools.search_dataset_records(api_client, dataset_id, query, limit)

@mcp.tool()
async def get_export_url(
    dataset_id: str,
    export_format: str = "csv",
    select: Optional[str] = None,
    where: Optional[str] = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """Get a URL for exporting dataset records in various formats.
    
    Args:
        dataset_id: Unique identifier for the dataset
        export_format: Export format (csv, json, geojson, etc.)
        select: ODSQL select clause
        where: ODSQL where clause
        group_by: ODSQL group by clause
        order_by: ODSQL order by clause
        limit: Maximum number of results
    """
    return await query_tools.get_export_url(
        api_client, dataset_id, export_format, select, where, group_by, order_by, limit
    )

"""
Analysis Tools - Data analysis and statistics
"""

@mcp.tool()
async def summarize_dataset(dataset_id: str) -> str:
    """Generate a comprehensive summary of a dataset including metadata, schema, and sample data.
    
    Args:
        dataset_id: Unique identifier for the dataset
    """
    return await analysis_tools.summarize_dataset(api_client, dataset_id)

@mcp.tool()
async def analyze_numeric_field(dataset_id: str, field_name: str) -> str:
    """Analyze a numeric field in a dataset, including min, max, average, and distribution.
    
    Args:
        dataset_id: Unique identifier for the dataset
        field_name: Name of the numeric field to analyze
    """
    return await analysis_tools.analyze_numeric_field(api_client, dataset_id, field_name)

@mcp.tool()
async def analyze_text_field(dataset_id: str, field_name: str, limit: int = 20) -> str:
    """Analyze a text field in a dataset, including value frequency.
    
    Args:
        dataset_id: Unique identifier for the dataset
        field_name: Name of the text field to analyze
        limit: Maximum number of unique values to analyze (default: 20)
    """
    return await analysis_tools.analyze_text_field(api_client, dataset_id, field_name, limit)

@mcp.tool()
async def analyze_date_field(dataset_id: str, field_name: str) -> str:
    """Analyze a date field in a dataset, including range, distribution by year/month.
    
    Args:
        dataset_id: Unique identifier for the dataset
        field_name: Name of the date field to analyze
    """
    return await analysis_tools.analyze_date_field(api_client, dataset_id, field_name)

@mcp.tool()
async def generate_dataset_statistics(dataset_id: str) -> str:
    """Generate comprehensive statistics for all fields in a dataset.
    
    Args:
        dataset_id: Unique identifier for the dataset
    """
    return await analysis_tools.generate_dataset_statistics(api_client, dataset_id)

if __name__ == "__main__":
    # Start the MCP server
    mcp.run(transport='stdio')