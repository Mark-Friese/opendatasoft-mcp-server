# Opendatasoft MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that provides tools for interacting with the [Opendatasoft Explore API v2.1](https://help.opendatasoft.com/apis/ods-explore-v2/), enabling AI assistants like Claude to search, query, and analyze open datasets.

## Features

- **Dataset Discovery**: Search and browse datasets by keywords, publishers, and themes
- **Dataset Exploration**: View schemas, metadata, and sample records
- **Data Querying**: Execute ODSQL queries with filtering, sorting, and aggregation
- **Data Analysis**: Generate statistics, analyze fields, and visualize distributions
- **Data Export**: Generate export URLs for various formats (CSV, JSON, GeoJSON, etc.)

## Installation

### Requirements

- Python 3.10 or later
- [Model Context Protocol (MCP) SDK](https://github.com/modelcontextprotocol/python-sdk) 1.2.0 or later
- Claude for Desktop or another MCP-compatible client

### Installing from Source

1. Clone the repository:
   ```
   git clone https://github.com/your-username/opendatasoft-mcp-server.git
   cd opendatasoft-mcp-server
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e .
   ```

## Configuration

The server can be configured using environment variables:

- `ODS_BASE_URL`: Base URL for the Opendatasoft domain (default: "https://documentation-resources.opendatasoft.com")
- `ODS_API_KEY`: API key for authenticated requests (optional)

## Usage with Claude for Desktop

1. Make sure you have Claude for Desktop installed. You can download it from [claude.ai/download](https://claude.ai/download).

2. Configure Claude for Desktop to use this MCP server by adding it to your Claude for Desktop configuration file:

   - On macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - On Windows: `%APPDATA%\Claude\claude_desktop_config.json`

   ```json
   {
     "mcpServers": {
       "opendatasoft": {
         "command": "/path/to/venv/bin/python",
         "args": ["-m", "src.main"],
         "env": {
           "ODS_BASE_URL": "https://documentation-resources.opendatasoft.com",
           "ODS_API_KEY": "your-api-key-if-needed"
         }
       }
     }
   }
   ```

3. Restart Claude for Desktop.

4. You can now use the Opendatasoft MCP server in your conversations with Claude.

## Available Tools

### Catalog Tools

- `search_datasets`: Search for datasets by keyword
- `get_dataset_info`: Get detailed information about a specific dataset
- `list_datasets_by_publisher`: List datasets from a specific publisher
- `list_dataset_fields`: List all fields in a dataset with their types and descriptions

### Query Tools

- `get_dataset_records`: Get records from a dataset with optional filtering and sorting
- `get_dataset_aggregates`: Get aggregated data from a dataset using ODSQL aggregation functions
- `facet_analysis`: Analyze facet values distribution for a dataset
- `search_dataset_records`: Search for specific records within a dataset
- `get_export_url`: Get a URL for exporting dataset records in various formats

### Analysis Tools

- `summarize_dataset`: Generate a comprehensive summary of a dataset
- `analyze_numeric_field`: Analyze a numeric field, including min, max, average, and distribution
- `analyze_text_field`: Analyze a text field, including value frequency
- `analyze_date_field`: Analyze a date field, including range, distribution by year/month
- `generate_dataset_statistics`: Generate comprehensive statistics for all fields in a dataset

## Example Queries for Claude

Here are some example queries you can ask Claude while using this MCP server:

- "Find datasets related to transportation."
- "Show me datasets published by the World Food Programme."
- "What are the fields in the 'world-administrative-boundaries' dataset?"
- "Get 5 records from the 'gold-prices' dataset."
- "Count the number of cities per country in the 'geonames-all-cities-with-a-population-1000' dataset."
- "Analyze the 'population' field in the 'world-administrative-boundaries' dataset."
- "What's the distribution of records by year in the 'gold-prices' dataset?"
- "Generate a CSV export URL for the 'gold-prices' dataset with prices sorted by date."

## Understanding ODSQL

Many of the tools in this MCP server use the Opendatasoft Query Language (ODSQL) for filtering, aggregating, and sorting data. Here are some basic examples:

### Select Clause

Choosing which fields to return:

```
select=field1, field2, field3
```

Aggregation:

```
select=count(*) as total, avg(field) as average
```

### Where Clause

Filtering records:

```
where=field > 100
where=date_field >= date'2020-01-01'
where=text_field like "Paris"
```

Full-text search:

```
where=search(field, "keyword")
```

### Group By Clause

Grouping results:

```
group_by=field
group_by=year(date_field)
```

### Order By Clause

Sorting results:

```
order_by=field ASC
order_by=field DESC
```

For more details on ODSQL syntax, see the [Opendatasoft documentation](https://help.opendatasoft.com/apis/ods-explore-v2/#section/Opendatasoft-Query-Language-(ODSQL)).

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Opendatasoft](https://www.opendatasoft.com/) for providing the Explore API
- [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) for enabling AI assistants to interact with tools
- [Claude](https://claude.ai/) for the AI assistant capabilities