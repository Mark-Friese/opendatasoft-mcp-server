# Opendatasoft MCP Server Usage Guide

This guide provides examples and best practices for using the Opendatasoft MCP server with Claude to explore open datasets.

## Quick Start

After setting up the MCP server with Claude for Desktop, you can start exploring Opendatasoft datasets using natural language queries. Here are some examples to get you started:

## Basic Dataset Discovery

### Finding Relevant Datasets

```
What datasets do you have related to climate change?
```

Claude will use the `search_datasets` tool to search for datasets with the relevant keywords.

### Exploring a Dataset's Structure

```
Tell me about the "gold-prices" dataset. What fields does it have?
```

Claude will use the `get_dataset_info` and `list_dataset_fields` tools to provide information about the dataset.

## Data Querying

### Getting Sample Records

```
Show me 5 records from the "gold-prices" dataset.
```

Claude will use the `get_dataset_records` tool to retrieve and display the records.

### Filtering Data

```
Show me gold prices from after 2010 with values higher than 1500.
```

Claude will construct an ODSQL query using the `where` parameter:

```
where=date > date'2010-01-01' AND price > 1500
```

### Aggregating Data

```
What was the average gold price per year?
```

Claude will use the `get_dataset_aggregates` tool with appropriate parameters:

```
select=year(date) as year, avg(price) as average_price
group_by=year(date)
order_by=year ASC
```

## Data Analysis

### Analyzing Fields

```
Analyze the "population" field in the "world-administrative-boundaries" dataset.
```

Claude will use the `analyze_numeric_field` tool to provide statistics and distribution information about the population field.

```
What are the most common names for cities in the "geonames-all-cities-with-a-population-1000" dataset?
```

Claude will use the `analyze_text_field` tool to analyze the frequency distribution of city names.

### Generating Dataset Summaries

```
Give me a summary of the "world-administrative-boundaries" dataset.
```

Claude will use the `summarize_dataset` tool to provide a comprehensive overview of the dataset.

## Advanced Usage

### Combining Multiple Tools

```
Find datasets about air quality, then show me the most recent records from the one with the most data points, and analyze the distribution of pollution levels.
```

Claude will execute a multi-step process:
1. Search for air quality datasets
2. Identify the most comprehensive dataset
3. Retrieve recent records
4. Analyze the pollution level distribution

### Data Export

```
Generate a CSV export URL for the gold prices from 2020 to 2023, sorted by date.
```

Claude will use the `get_export_url` tool with appropriate parameters:

```
export_format=csv
where=date >= date'2020-01-01' AND date <= date'2023-12-31'
order_by=date ASC
```

## ODSQL Quick Reference

When working with the Opendatasoft API, you may want to use specific ODSQL syntax for more precise queries. Here are some common patterns:

### Field Selection

```
select=field1, field2, field3
```

### Text Filtering

```
where=field = "exact match"
where=field like "partial match"
```

### Numeric Filtering

```
where=field > 100
where=field >= 100 AND field <= 200
```

### Date Filtering

```
where=date_field > date'2020-01-01'
where=date_field BETWEEN date'2020-01-01' AND date'2020-12-31'
```

### Date Functions

```
year(date_field)    # Extract year
month(date_field)   # Extract month (1-12)
day(date_field)     # Extract day of month
```

### Aggregation Functions

```
count(*)            # Count records
sum(field)          # Sum values
avg(field)          # Average of values
min(field)          # Minimum value
max(field)          # Maximum value
```

## Tips for Effective Usage

1. **Start broad, then narrow down**: Begin with dataset discovery, then explore specific datasets, and finally drill down into particular fields or records.

2. **Be specific about your information needs**: Instead of asking "Tell me about transportation data," ask "What datasets do you have on public transportation, and what fields do they contain?"

3. **Specify record limits**: When retrieving records, specify a reasonable limit (e.g., "Show me the first 10 records").

4. **Reference previous results**: You can ask Claude to analyze data from previously retrieved datasets (e.g., "Now analyze the population field from that dataset").

5. **Request visualizations through descriptions**: While Claude can't directly create visual charts, it can describe data trends and distributions (e.g., "Describe the distribution of city populations").

## Troubleshooting

### If Claude doesn't use the right tool:

Try being more explicit about what you want, for example:
- "Use the search_datasets tool to find datasets about climate change."
- "Please use the analyze_numeric_field tool on the population field."

### If you get error messages:

- **Dataset not found**: Verify the dataset ID is correct. You can use `search_datasets` to find available datasets.
- **Field not found**: Use `list_dataset_fields` to see available fields in a dataset.
- **Query errors**: ODSQL syntax may be incorrect. Simplify your query or refer to the ODSQL reference.

## Example Workflows

### Geographic Data Analysis

```
1. Find datasets with geographic information.
2. Get information about the "world-administrative-boundaries" dataset.
3. Show me 5 records from this dataset.
4. What countries have the highest populations?
5. Analyze the distribution of countries by continent.
6. Generate a GeoJSON export URL for countries in Asia.
```

### Time Series Analysis

```
1. Find datasets with historical price data.
2. Tell me about the "gold-prices" dataset.
3. What's the date range for this dataset?
4. Show me the average gold price per year.
5. When did gold reach its highest price?
6. How does the price trend compare before and after 2008?
```