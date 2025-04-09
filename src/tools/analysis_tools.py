"""
Tools for analyzing and generating insights from Opendatasoft datasets.
"""
from typing import List, Dict, Any, Optional
import json
from ods_api import OdsApiClient

async def summarize_dataset(
    api_client: OdsApiClient,
    dataset_id: str
) -> str:
    """
    Generate a comprehensive summary of a dataset including metadata, schema, and sample data.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        
    Returns:
        Formatted string with dataset summary
    """
    # Get dataset metadata
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
    except Exception as e:
        return f"Error retrieving dataset information: {str(e)}"
    
    if not dataset_info:
        return f"Dataset with ID '{dataset_id}' not found."
    
    # Extract basic metadata
    metas = dataset_info.get("metas", {}).get("default", {})
    title = metas.get("title", "Untitled Dataset")
    publisher = metas.get("publisher", "Unknown Publisher")
    description = metas.get("description", "No description available.")
    records_count = metas.get("records_count", "Unknown")
    theme = metas.get("theme", [""])[0] if metas.get("theme") else ""
    license = metas.get("license", "Unknown License")
    
    # Clean up description - remove HTML tags
    description = description.replace("<p>", "").replace("</p>", " ").replace("<br>", " ")
    
    # Get fields information
    fields = dataset_info.get("fields", [])
    
    # Get sample records
    try:
        records_data = await api_client.get_dataset_records(dataset_id, limit=5)
        sample_records = records_data.get("results", [])
    except:
        sample_records = []
    
    # Build summary
    output = [
        f"# Dataset Summary: {title}",
        f"\n## Basic Information",
        f"- **Dataset ID**: {dataset_id}",
        f"- **Publisher**: {publisher}",
        f"- **Theme**: {theme}",
        f"- **License**: {license}",
        f"- **Records Count**: {records_count}",
        f"\n## Description",
        description,
        f"\n## Schema ({len(fields)} fields)"
    ]
    
    # List all fields with types
    field_types = {}
    for field in fields:
        name = field.get("name", "Unnamed")
        field_type = field.get("type", "Unknown")
        field_types[field_type] = field_types.get(field_type, 0) + 1
        
        label = field.get("label", name)
        output.append(f"- **{label}** ({name}): {field_type}")
    
    # Add field type distribution
    output.append(f"\n## Field Type Distribution")
    for field_type, count in field_types.items():
        output.append(f"- {field_type}: {count} fields")
    
    # Add sample records if available
    if sample_records:
        output.append(f"\n## Sample Records (5 of {records_count})")
        
        for i, record in enumerate(sample_records, 1):
            output.append(f"\n### Record {i}")
            for key, value in record.items():
                if isinstance(value, dict) or isinstance(value, list):
                    value = json.dumps(value, ensure_ascii=False)
                output.append(f"- **{key}**: {value}")
    
    return "\n".join(output)

async def analyze_numeric_field(
    api_client: OdsApiClient,
    dataset_id: str,
    field_name: str
) -> str:
    """
    Analyze a numeric field in a dataset, including min, max, average, and distribution.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        field_name: Field name to analyze
        
    Returns:
        Formatted string with field analysis
    """
    # First, verify the field exists and is numeric
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        fields = dataset_info.get("fields", [])
        
        # Find the specified field
        field_info = None
        for field in fields:
            if field.get("name") == field_name:
                field_info = field
                break
        
        if not field_info:
            return f"Field '{field_name}' not found in dataset '{dataset_id}'."
        
        field_type = field_info.get("type", "")
        if field_type not in ["int", "double", "decimal", "float"]:
            return f"Field '{field_name}' is not a numeric field (type: {field_type})."
        
        field_label = field_info.get("label", field_name)
    except Exception as e:
        return f"Error validating field: {str(e)}"
    
    # Get basic statistics using aggregation
    try:
        stats = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=f"min({field_name}) as min, max({field_name}) as max, avg({field_name}) as avg, count({field_name}) as count"
        )
        
        if not stats or "results" not in stats or not stats["results"]:
            return f"Failed to compute statistics for field '{field_name}'."
        
        stat_values = stats["results"][0]
        min_val = stat_values.get("min", "N/A")
        max_val = stat_values.get("max", "N/A")
        avg_val = stat_values.get("avg", "N/A")
        count = stat_values.get("count", "N/A")
    except Exception as e:
        return f"Error computing field statistics: {str(e)}"
    
    # Get dataset info to include in the output
    dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    
    # Get distribution information (create 10 ranges between min and max)
    try:
        # Only attempt distribution if we have valid min/max values
        if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
            range_size = (max_val - min_val) / 10
            
            # If range_size is zero (min == max), we can't create a distribution
            if range_size > 0:
                distribution = []
                
                for i in range(10):
                    lower = min_val + (i * range_size)
                    upper = min_val + ((i + 1) * range_size)
                    
                    # Get count in this range
                    where_clause = f"{field_name} >= {lower} AND {field_name} < {upper}"
                    if i == 9:  # Include the max value in the last range
                        where_clause = f"{field_name} >= {lower} AND {field_name} <= {upper}"
                    
                    range_result = await api_client.get_dataset_records(
                        dataset_id=dataset_id,
                        select=f"count(*) as count",
                        where=where_clause
                    )
                    
                    range_count = range_result.get("results", [{}])[0].get("count", 0)
                    distribution.append((lower, upper, range_count))
            else:
                distribution = None
        else:
            distribution = None
    except Exception as e:
        distribution = None
    
    # Build output
    output = [
        f"# Analysis of {field_label} ({field_name})",
        f"\nDataset: {dataset_title} (ID: {dataset_id})",
        f"\n## Basic Statistics",
        f"- **Count**: {count}",
        f"- **Minimum**: {min_val}",
        f"- **Maximum**: {max_val}",
        f"- **Average**: {avg_val}"
    ]
    
    # Add distribution if available
    if distribution:
        output.append(f"\n## Value Distribution")
        output.append("| Range | Count |")
        output.append("| --- | --- |")
        
        for lower, upper, range_count in distribution:
            output.append(f"| {lower:.2f} - {upper:.2f} | {range_count} |")
    
    return "\n".join(output)

async def analyze_text_field(
    api_client: OdsApiClient,
    dataset_id: str,
    field_name: str,
    limit: int = 20
) -> str:
    """
    Analyze a text field in a dataset, including value frequency.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        field_name: Field name to analyze
        limit: Maximum number of unique values to analyze
        
    Returns:
        Formatted string with field analysis
    """
    # First, verify the field exists and is text
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        fields = dataset_info.get("fields", [])
        
        # Find the specified field
        field_info = None
        for field in fields:
            if field.get("name") == field_name:
                field_info = field
                break
        
        if not field_info:
            return f"Field '{field_name}' not found in dataset '{dataset_id}'."
        
        field_type = field_info.get("type", "")
        if field_type != "text":
            return f"Field '{field_name}' is not a text field (type: {field_type})."
        
        field_label = field_info.get("label", field_name)
    except Exception as e:
        return f"Error validating field: {str(e)}"
    
    # Get value frequency using aggregation
    try:
        frequency = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=f"{field_name}, count(*) as count",
            group_by=field_name,
            order_by="count DESC",
            limit=limit
        )
        
        if not frequency or "results" not in frequency or not frequency["results"]:
            return f"Failed to compute value frequency for field '{field_name}'."
        
        values = frequency["results"]
    except Exception as e:
        return f"Error computing value frequency: {str(e)}"
    
    # Get total records count
    try:
        count_result = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select="count(*) as total"
        )
        total_records = count_result.get("results", [{}])[0].get("total", 0)
    except:
        total_records = "Unknown"
    
    # Get dataset info to include in the output
    dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    
    # Get distinct value count
    try:
        distinct_result = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=f"count(distinct {field_name}) as distinct_count"
        )
        distinct_count = distinct_result.get("results", [{}])[0].get("distinct_count", "Unknown")
    except:
        distinct_count = "Unknown"
    
    # Build output
    output = [
        f"# Analysis of {field_label} ({field_name})",
        f"\nDataset: {dataset_title} (ID: {dataset_id})",
        f"\n## Basic Statistics",
        f"- **Total Records**: {total_records}",
        f"- **Distinct Values**: {distinct_count}"
    ]
    
    # Add value frequency
    if values:
        output.append(f"\n## Top {len(values)} Values by Frequency")
        output.append("| Value | Count | Percentage |")
        output.append("| --- | --- | --- |")
        
        for value_info in values:
            value = value_info.get(field_name, "N/A")
            count = value_info.get("count", 0)
            
            # Calculate percentage if total_records is a number
            if isinstance(total_records, (int, float)) and total_records > 0:
                percentage = (count / total_records) * 100
                percentage_str = f"{percentage:.2f}%"
            else:
                percentage_str = "N/A"
            
            output.append(f"| {value} | {count} | {percentage_str} |")
    
    return "\n".join(output)

async def analyze_date_field(
    api_client: OdsApiClient,
    dataset_id: str,
    field_name: str
) -> str:
    """
    Analyze a date field in a dataset, including range, distribution by year/month.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        field_name: Field name to analyze
        
    Returns:
        Formatted string with field analysis
    """
    # First, verify the field exists and is a date field
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
        fields = dataset_info.get("fields", [])
        
        # Find the specified field
        field_info = None
        for field in fields:
            if field.get("name") == field_name:
                field_info = field
                break
        
        if not field_info:
            return f"Field '{field_name}' not found in dataset '{dataset_id}'."
        
        field_type = field_info.get("type", "")
        if field_type not in ["date", "datetime"]:
            return f"Field '{field_name}' is not a date field (type: {field_type})."
        
        field_label = field_info.get("label", field_name)
    except Exception as e:
        return f"Error validating field: {str(e)}"
    
    # Get basic statistics using aggregation
    try:
        stats = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=f"min({field_name}) as min_date, max({field_name}) as max_date, count({field_name}) as count"
        )
        
        if not stats or "results" not in stats or not stats["results"]:
            return f"Failed to compute statistics for field '{field_name}'."
        
        stat_values = stats["results"][0]
        min_date = stat_values.get("min_date", "N/A")
        max_date = stat_values.get("max_date", "N/A")
        count = stat_values.get("count", "N/A")
    except Exception as e:
        return f"Error computing field statistics: {str(e)}"
    
    # Get dataset info to include in the output
    dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    
    # Get distribution by year
    try:
        year_distribution = await api_client.get_dataset_records(
            dataset_id=dataset_id,
            select=f"year({field_name}) as year, count(*) as count",
            group_by=f"year({field_name})",
            order_by="year"
        )
        
        years_data = year_distribution.get("results", [])
    except:
        years_data = []
    
    # Get distribution by month (limited to 5 years if possible)
    month_data = []
    if years_data:
        try:
            # Get the most recent years (up to 5)
            recent_years = sorted([item.get("year") for item in years_data], reverse=True)[:5]
            
            # For each recent year, get month distribution
            for year in recent_years:
                month_distribution = await api_client.get_dataset_records(
                    dataset_id=dataset_id,
                    select=f"month({field_name}) as month, count(*) as count",
                    where=f"year({field_name}) = {year}",
                    group_by=f"month({field_name})",
                    order_by="month"
                )
                
                months = month_distribution.get("results", [])
                if months:
                    month_data.append((year, months))
        except:
            pass
    
    # Build output
    output = [
        f"# Analysis of {field_label} ({field_name})",
        f"\nDataset: {dataset_title} (ID: {dataset_id})",
        f"\n## Basic Statistics",
        f"- **Count**: {count}",
        f"- **Earliest Date**: {min_date}",
        f"- **Latest Date**: {max_date}"
    ]
    
    # Add year distribution
    if years_data:
        output.append(f"\n## Distribution by Year")
        output.append("| Year | Count |")
        output.append("| --- | --- |")
        
        for year_info in years_data:
            year = year_info.get("year", "N/A")
            year_count = year_info.get("count", 0)
            output.append(f"| {year} | {year_count} |")
    
    # Add month distribution for recent years
    if month_data:
        output.append(f"\n## Monthly Distribution (Last {len(month_data)} Years)")
        
        for year, months in month_data:
            output.append(f"\n### {year}")
            output.append("| Month | Count |")
            output.append("| --- | --- |")
            
            for month_info in months:
                month_num = month_info.get("month", 0)
                month_name = ["January", "February", "March", "April", "May", "June", 
                             "July", "August", "September", "October", "November", "December"][month_num-1]
                month_count = month_info.get("count", 0)
                output.append(f"| {month_name} | {month_count} |")
    
    return "\n".join(output)

async def generate_dataset_statistics(
    api_client: OdsApiClient,
    dataset_id: str
) -> str:
    """
    Generate comprehensive statistics for all fields in a dataset.
    
    Args:
        api_client: OdsApiClient instance
        dataset_id: Dataset identifier
        
    Returns:
        Formatted string with dataset statistics
    """
    # Get dataset information including fields
    try:
        dataset_info = await api_client.get_dataset(dataset_id)
    except Exception as e:
        return f"Error retrieving dataset information: {str(e)}"
    
    if not dataset_info:
        return f"Dataset with ID '{dataset_id}' not found."
    
    # Extract basic metadata
    dataset_title = dataset_info.get("metas", {}).get("default", {}).get("title", "Unknown Dataset")
    fields = dataset_info.get("fields", [])
    
    if not fields:
        return f"No fields found for dataset '{dataset_id}'."
    
    # Group fields by type
    field_groups = {
        "numeric": [],
        "text": [],
        "date": [],
        "geo": [],
        "other": []
    }
    
    for field in fields:
        field_name = field.get("name", "")
        field_type = field.get("type", "")
        field_label = field.get("label", field_name)
        
        if field_type in ["int", "double", "decimal", "float"]:
            field_groups["numeric"].append((field_name, field_label, field_type))
        elif field_type == "text":
            field_groups["text"].append((field_name, field_label, field_type))
        elif field_type in ["date", "datetime"]:
            field_groups["date"].append((field_name, field_label, field_type))
        elif field_type in ["geo_point_2d", "geo_shape"]:
            field_groups["geo"].append((field_name, field_label, field_type))
        else:
            field_groups["other"].append((field_name, field_label, field_type))
    
    # Build output
    output = [
        f"# Dataset Statistics: {dataset_title}",
        f"\nDataset ID: {dataset_id}",
        f"\n## Field Count by Type",
        f"- **Numeric Fields**: {len(field_groups['numeric'])}",
        f"- **Text Fields**: {len(field_groups['text'])}",
        f"- **Date Fields**: {len(field_groups['date'])}",
        f"- **Geographic Fields**: {len(field_groups['geo'])}",
        f"- **Other Fields**: {len(field_groups['other'])}",
        f"\n## Detailed Field Information"
    ]
    
    # Generate statistics for each field type
    
    # Numeric Fields
    if field_groups["numeric"]:
        output.append(f"\n### Numeric Fields")
        
        # Get basic stats for all numeric fields at once
        numeric_field_names = [f[0] for f in field_groups["numeric"]]
        select_clauses = []
        
        for field_name in numeric_field_names:
            select_clauses.append(f"min({field_name}) as min_{field_name}")
            select_clauses.append(f"max({field_name}) as max_{field_name}")
            select_clauses.append(f"avg({field_name}) as avg_{field_name}")
            select_clauses.append(f"count({field_name}) as count_{field_name}")
        
        try:
            stats = await api_client.get_dataset_records(
                dataset_id=dataset_id,
                select=", ".join(select_clauses)
            )
            
            stat_values = stats.get("results", [{}])[0] if stats and "results" in stats else {}
        except:
            stat_values = {}
        
        output.append("| Field | Type | Count | Min | Max | Average |")
        output.append("| --- | --- | --- | --- | --- | --- |")
        
        for field_name, field_label, field_type in field_groups["numeric"]:
            min_val = stat_values.get(f"min_{field_name}", "N/A")
            max_val = stat_values.get(f"max_{field_name}", "N/A")
            avg_val = stat_values.get(f"avg_{field_name}", "N/A")
            count = stat_values.get(f"count_{field_name}", "N/A")
            
            output.append(f"| {field_label} ({field_name}) | {field_type} | {count} | {min_val} | {max_val} | {avg_val} |")
    
    # Text Fields
    if field_groups["text"]:
        output.append(f"\n### Text Fields")
        output.append("| Field | Distinct Values | Fill Rate |")
        output.append("| --- | --- | --- |")
        
        # Get total record count
        try:
            count_result = await api_client.get_dataset_records(
                dataset_id=dataset_id,
                select="count(*) as total"
            )
            total_records = count_result.get("results", [{}])[0].get("total", 0)
        except:
            total_records = 0
        
        for field_name, field_label, field_type in field_groups["text"]:
            try:
                # Get distinct count
                distinct_result = await api_client.get_dataset_records(
                    dataset_id=dataset_id,
                    select=f"count(distinct {field_name}) as distinct_count, count({field_name}) as count"
                )
                
                distinct_count = distinct_result.get("results", [{}])[0].get("distinct_count", "N/A")
                field_count = distinct_result.get("results", [{}])[0].get("count", 0)
                
                # Calculate fill rate
                if isinstance(field_count, (int, float)) and isinstance(total_records, (int, float)) and total_records > 0:
                    fill_rate = (field_count / total_records) * 100
                    fill_rate_str = f"{fill_rate:.2f}%"
                else:
                    fill_rate_str = "N/A"
            except:
                distinct_count = "N/A"
                fill_rate_str = "N/A"
            
            output.append(f"| {field_label} ({field_name}) | {distinct_count} | {fill_rate_str} |")
    
    # Date Fields
    if field_groups["date"]:
        output.append(f"\n### Date Fields")
        output.append("| Field | Earliest Date | Latest Date | Fill Rate |")
        output.append("| --- | --- | --- | --- |")
        
        # Get total record count (if not already calculated)
        if not isinstance(total_records, (int, float)):
            try:
                count_result = await api_client.get_dataset_records(
                    dataset_id=dataset_id,
                    select="count(*) as total"
                )
                total_records = count_result.get("results", [{}])[0].get("total", 0)
            except:
                total_records = 0
        
        for field_name, field_label, field_type in field_groups["date"]:
            try:
                # Get date range and count
                date_result = await api_client.get_dataset_records(
                    dataset_id=dataset_id,
                    select=f"min({field_name}) as min_date, max({field_name}) as max_date, count({field_name}) as count"
                )
                
                min_date = date_result.get("results", [{}])[0].get("min_date", "N/A")
                max_date = date_result.get("results", [{}])[0].get("max_date", "N/A")
                field_count = date_result.get("results", [{}])[0].get("count", 0)
                
                # Calculate fill rate
                if isinstance(field_count, (int, float)) and isinstance(total_records, (int, float)) and total_records > 0:
                    fill_rate = (field_count / total_records) * 100
                    fill_rate_str = f"{fill_rate:.2f}%"
                else:
                    fill_rate_str = "N/A"
            except:
                min_date = "N/A"
                max_date = "N/A"
                fill_rate_str = "N/A"
            
            output.append(f"| {field_label} ({field_name}) | {min_date} | {max_date} | {fill_rate_str} |")
    
    # Geographic Fields
    if field_groups["geo"]:
        output.append(f"\n### Geographic Fields")
        output.append("| Field | Type | Fill Rate |")
        output.append("| --- | --- | --- |")
        
        # Get total record count (if not already calculated)
        if not isinstance(total_records, (int, float)):
            try:
                count_result = await api_client.get_dataset_records(
                    dataset_id=dataset_id,
                    select="count(*) as total"
                )
                total_records = count_result.get("results", [{}])[0].get("total", 0)
            except:
                total_records = 0
        
        for field_name, field_label, field_type in field_groups["geo"]:
            try:
                # Get count for this field
                geo_result = await api_client.get_dataset_records(
                    dataset_id=dataset_id,
                    select=f"count({field_name}) as count"
                )
                
                field_count = geo_result.get("results", [{}])[0].get("count", 0)
                
                # Calculate fill rate
                if isinstance(field_count, (int, float)) and isinstance(total_records, (int, float)) and total_records > 0:
                    fill_rate = (field_count / total_records) * 100
                    fill_rate_str = f"{fill_rate:.2f}%"
                else:
                    fill_rate_str = "N/A"
            except:
                fill_rate_str = "N/A"
            
            output.append(f"| {field_label} ({field_name}) | {field_type} | {fill_rate_str} |")
    
    # Other Fields
    if field_groups["other"]:
        output.append(f"\n### Other Fields")
        output.append("| Field | Type |")
        output.append("| --- | --- |")
        
        for field_name, field_label, field_type in field_groups["other"]:
            output.append(f"| {field_label} ({field_name}) | {field_type} |")
    
    return "\n".join(output)