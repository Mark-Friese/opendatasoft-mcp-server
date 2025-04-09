"""
Opendatasoft API client for MCP server.
Provides methods to interact with the Opendatasoft Explore API v2.1.
"""
import httpx
from typing import Dict, Any, List, Optional, Union
import urllib.parse

class OdsApiClient:
    """
    Client for the Opendatasoft Explore API v2.1.
    
    Attributes:
        base_url: Base URL for the Opendatasoft domain
        api_key: Optional API key for authenticated requests
    """
    
    def __init__(self, base_url: str = "https://documentation-resources.opendatasoft.com", api_key: Optional[str] = None):
        """
        Initialize the Opendatasoft API client.
        
        Args:
            base_url: Base URL for the Opendatasoft domain
            api_key: Optional API key for authenticated requests
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.api_path = "/api/explore/v2.1"
        
    async def _make_request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a request to the Opendatasoft API.
        
        Args:
            path: API endpoint path
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{self.api_path}{path}"
        
        # Add API key if provided
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Apikey {self.api_key}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
    
    async def list_datasets(self, 
                      search: Optional[str] = None,
                      publisher: Optional[str] = None, 
                      theme: Optional[str] = None,
                      where: Optional[str] = None,
                      limit: int = 10,
                      offset: int = 0) -> Dict[str, Any]:
        """
        List datasets from the catalog.
        
        Args:
            search: Full-text search query
            publisher: Filter by publisher
            theme: Filter by theme
            where: ODSQL where clause
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            Dictionary containing the list of datasets
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        # Build where clause from parameters
        where_clauses = []
        if where:
            where_clauses.append(where)
        if publisher:
            where_clauses.append(f'publisher="{publisher}"')
        if theme:
            where_clauses.append(f'theme="{theme}"')
            
        if where_clauses:
            params["where"] = " AND ".join(where_clauses)
            
        # Add search parameter if provided
        if search:
            params["where"] = f'"{search}"' + (f' AND {params["where"]}' if "where" in params else "")
        
        return await self._make_request("/catalog/datasets", params)
    
    async def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific dataset.
        
        Args:
            dataset_id: Dataset identifier
            
        Returns:
            Dictionary containing dataset information
        """
        path = f"/catalog/datasets/{dataset_id}"
        return await self._make_request(path)
    
    async def get_dataset_records(self, 
                           dataset_id: str,
                           select: Optional[str] = None,
                           where: Optional[str] = None,
                           group_by: Optional[str] = None,
                           order_by: Optional[str] = None,
                           limit: int = 10,
                           offset: int = 0) -> Dict[str, Any]:
        """
        Get records from a dataset.
        
        Args:
            dataset_id: Dataset identifier
            select: ODSQL select clause
            where: ODSQL where clause
            group_by: ODSQL group by clause
            order_by: ODSQL order by clause
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            Dictionary containing dataset records
        """
        path = f"/catalog/datasets/{dataset_id}/records"
        
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if select:
            params["select"] = select
        if where:
            params["where"] = where
        if group_by:
            params["group_by"] = group_by
        if order_by:
            params["order_by"] = order_by
            
        return await self._make_request(path, params)
    
    async def get_dataset_facets(self, 
                          dataset_id: str,
                          facet: List[str],
                          where: Optional[str] = None) -> Dict[str, Any]:
        """
        Get facet values for a dataset.
        
        Args:
            dataset_id: Dataset identifier
            facet: List of facet fields
            where: ODSQL where clause
            
        Returns:
            Dictionary containing facet values
        """
        path = f"/catalog/datasets/{dataset_id}/facets"
        
        params = {"facet": facet}
        if where:
            params["where"] = where
            
        return await self._make_request(path, params)
    
    async def export_records(self,
                      dataset_id: str,
                      export_format: str = "csv",
                      select: Optional[str] = None,
                      where: Optional[str] = None,
                      group_by: Optional[str] = None,
                      order_by: Optional[str] = None,
                      limit: Optional[int] = None) -> str:
        """
        Get export URL for dataset records.
        
        Args:
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
        path = f"/catalog/datasets/{dataset_id}/exports/{export_format}"
        
        params = {}
        if select:
            params["select"] = select
        if where:
            params["where"] = where
        if group_by:
            params["group_by"] = group_by
        if order_by:
            params["order_by"] = order_by
        if limit is not None:
            params["limit"] = limit
        
        # Instead of making the request directly, return the URL
        query_string = urllib.parse.urlencode(params)
        return f"{self.base_url}{self.api_path}{path}?{query_string}"
    
    async def search_datasets(self, query: str, limit: int = 10) -> Dict[str, Any]:
        """
        Search for datasets by keyword.
        
        Args:
            query: Search query
            limit: Maximum number of results
            
        Returns:
            Dictionary containing search results
        """
        return await self.list_datasets(search=query, limit=limit)
    
    async def search_records(self, 
                      dataset_id: str, 
                      query: str,
                      limit: int = 10) -> Dict[str, Any]:
        """
        Search for records within a dataset.
        
        Args:
            dataset_id: Dataset identifier
            query: Search query
            limit: Maximum number of results
            
        Returns:
            Dictionary containing search results
        """
        return await self.get_dataset_records(
            dataset_id=dataset_id,
            where=f'search({query})',
            limit=limit
        )