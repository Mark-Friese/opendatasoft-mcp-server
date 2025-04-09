"""
Test script for the Opendatasoft MCP Server.

This script tests the server by making direct calls to the MCP tools.
"""
import asyncio
import sys
from ods_api import OdsApiClient
from tools import catalog_tools, query_tools, analysis_tools

async def test_catalog_tools():
    """Test catalog tools"""
    print("=== Testing Catalog Tools ===")
    
    # Initialize API client
    api_client = OdsApiClient()
    
    # Test search_datasets
    print("\n>> Testing search_datasets")
    result = await catalog_tools.search_datasets(api_client, "demand", 3)
    print(result)
    
    # Test get_dataset_info
    print("\n>> Testing get_dataset_info")
    result = await catalog_tools.get_dataset_info(api_client, "spd-ltds-appendix-3-system-loads-table-3")
    print(result)
    
    # Test list_dataset_fields
    print("\n>> Testing list_dataset_fields")
    result = await catalog_tools.list_dataset_fields(api_client, "spd-ltds-appendix-3-system-loads-table-3")
    print(result)

async def test_query_tools():
    """Test query tools"""
    print("\n=== Testing Query Tools ===")
    
    # Initialize API client
    api_client = OdsApiClient()
    
    # Test get_dataset_records
    print("\n>> Testing get_dataset_records")
    result = await query_tools.get_dataset_records(
        api_client, "spd-ltds-appendix-3-system-loads-table-3", limit=3
    )
    print(result)
    
    # Test get_dataset_aggregates
    print("\n>> Testing get_dataset_aggregates")
    result = await query_tools.get_dataset_aggregates(
        api_client, "spd-ltds-appendix-3-system-loads-table-3", 
        select="year(date) as year, avg(price) as avg_price",
        group_by="year(date)",
        limit=5
    )
    print(result)

async def test_analysis_tools():
    """Test analysis tools"""
    print("\n=== Testing Analysis Tools ===")
    
    # Initialize API client
    api_client = OdsApiClient()
    
    # Test summarize_dataset
    print("\n>> Testing summarize_dataset")
    result = await analysis_tools.summarize_dataset(api_client, "gold-prices")
    print(result)

async def main():
    """Main test function"""
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        if test_name == "catalog":
            await test_catalog_tools()
        elif test_name == "query":
            await test_query_tools()
        elif test_name == "analysis":
            await test_analysis_tools()
        else:
            print(f"Unknown test: {test_name}")
            print("Available tests: catalog, query, analysis")
    else:
        # Run all tests
        await test_catalog_tools()
        await test_query_tools()
        await test_analysis_tools()

if __name__ == "__main__":
    asyncio.run(main())