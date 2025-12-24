from typing import Dict, List
from pydantic import BaseModel, Field


class ColumnHierarchy(BaseModel):
    """Column hierarchy for filter selection"""
    column_id: str = Field(..., description="Column UUID")
    column_name: str = Field(..., description="Column name")
    table_id: str = Field(..., description="Table UUID")
    table_name: str = Field(..., description="Table name")
    table_group_id: str = Field(..., description="Table group UUID")
    table_group_name: str = Field(..., description="Table group name")


class FilterOptions(BaseModel):
    """All available filter options for dashboard creation"""
    
    # Filter field metadata (labels for UI display)
    filter_fields_metadata: List[Dict[str, str]] = Field(
        ...,
        description="Metadata for filter fields including labels",
        example=[
            {"field": "table_groups_name", "label": "Table Group"},
            {"field": "business_domain", "label": "Business Domain"}
        ]
    )
    
    # Filter field values (for "Filter by" dropdowns)
    filter_values: Dict[str, List[str]] = Field(
        ...,
        description="Available values for each filter field",
        example={
            "table_groups_name": ["demo", "production"],
            "data_source": ["postgres", "snowflake"],
        }
    )
    
    # Column hierarchy (for "Selected Columns" filter)
    columns: List[ColumnHierarchy] = Field(
        ...,
        description="Hierarchical list of table groups → tables → columns"
    )
    
    # Category options (for "Display on scorecard" dropdown)
    category_options: List[Dict[str, str]] = Field(
        ...,
        description="Available categories for scorecard display",
        example=[
            {"value": "dq_dimension", "label": "Quality Dimension"},
            {"value": "table_groups_name", "label": "Table Group"}
        ]
    )
    
    # Score grouping options (for "Score grouped by" dropdown)
    score_grouping_options: List[Dict[str, str]] = Field(
        ...,
        description="Available options for score grouping",
        example=[
            {"value": "table_groups_name", "label": "Table Group"},
            {"value": "table_name", "label": "Table"},
            {"value": "column_name", "label": "Column"}
        ]
    )
    
    # Score type options (for "Total Score" / "CDE Score" selection)
    score_type_options: List[Dict[str, str]] = Field(
        ...,
        description="Available score types",
        example=[
            {"value": "score", "label": "Total Score"},
            {"value": "cde_score", "label": "CDE Score"}
        ]
    )
