"""Data Quality Dashboard API Router"""
import sys
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

# Add the dataops-testgen directory to Python path
testgen_path = Path(__file__).parent.parent.parent / "dataops-testgen"
if str(testgen_path) not in sys.path:
    sys.path.insert(0, str(testgen_path))

from testgen.commands.run_refresh_score_cards_results import run_refresh_score_cards_results
from testgen.common.models.scores import (
    ScoreDefinition,
    ScoreDefinitionCriteria,
    ScoreCategory,
)
from testgen.ui.services.database_service import fetch_all_from_db
from testgen.utils import format_score_card, format_score_card_breakdown, format_score_card_issues

from src.dependencies import get_db_session, validate_project_code, validate_dashboard_id
from src.schemas.data_quality import (
    DashboardCreate,
    DashboardUpdate,
    DashboardResponse,
    DashboardSummary,
    BreakdownItem,
    IssueItem,
    RecalculateResponse,
)
from src.schemas.filter_options import FilterOptions, ColumnHierarchy

# Valid group_by categories for breakdown and issues endpoints
class GroupByCategory(str, Enum):
    """Valid categories for grouping dashboard breakdowns and issues."""
    column_name = "column_name"
    table_name = "table_name"
    dq_dimension = "dq_dimension"
    semantic_data_type = "semantic_data_type"
    table_groups_name = "table_groups_name"
    data_location = "data_location"
    data_source = "data_source"
    source_system = "source_system"
    source_process = "source_process"
    business_domain = "business_domain"
    stakeholder_group = "stakeholder_group"
    transform_level = "transform_level"
    data_product = "data_product"

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


@router.post("/dashboards", response_model=DashboardResponse, status_code=status.HTTP_201_CREATED)
def create_dashboard(
    dashboard: DashboardCreate,
    db: Session = Depends(get_db_session)
):
    """
    Create a new data quality dashboard.
    
    Args:
        dashboard: Dashboard configuration
        db: Database session
        
    Returns:
        Created dashboard with initial scores
    """
    # Validate project exists
    validate_project_code(dashboard.project_code, db)
    
    # Create score definition
    score_def = ScoreDefinition()
    score_def.project_code = dashboard.project_code
    score_def.name = dashboard.name
    score_def.total_score = dashboard.total_score
    score_def.cde_score = dashboard.cde_score
    
    if dashboard.category:
        score_def.category = ScoreCategory[dashboard.category]
    
    # Create criteria with filters
    filters_data = [
        {
            "field": f.field,
            "value": f.value,
            "others": f.others
        }
        for f in dashboard.filters
    ]
    score_def.criteria = ScoreDefinitionCriteria.from_filters(
        filters_data,
        group_by_field=dashboard.group_by_field
    )
    
    # Save to database
    score_def.save()
    
    # Get fresh score card and cache it to database
    score_card = score_def.as_score_card(save_to_cache=True)
    formatted = format_score_card(score_card)
    
    return DashboardResponse(**formatted)



@router.get("/dashboards", response_model=List[DashboardResponse])
def list_dashboards(
    project_code: Optional[str] = Query(None, description="Filter by project code"),
    name_filter: Optional[str] = Query(None, description="Filter by name (case-insensitive)"),
    sorted_by: str = Query("name", description="Sort field"),
    include_scores: bool = Query(False, description="Include full score details (score, profiling_score, testing_score, categories)"),
    include_history: bool = Query(False, description="Include historical score data for graphs"),
    db: Session = Depends(get_db_session)
):
    """
    List all data quality dashboards with optional filters and details.
    
    Args:
        project_code: Optional project code filter
        name_filter: Optional name filter
        sorted_by: Sort field (default: name)
        include_scores: Include full score details (default: False)
        include_history: Include historical score data (default: False)
        db: Database session
        
    Returns:
        List of dashboards (summary or full details based on parameters)
        
    Examples:
        # Get basic list (id, name, project_code, total_score flag, cde_score flag, category)
        GET /api/data-quality/dashboards?project_code=DEFAULT
        
        # Get list with full scores (includes score values and categories)
        GET /api/data-quality/dashboards?project_code=DEFAULT&include_scores=true
        
        # Get list with scores and history (for rendering graphs)
        GET /api/data-quality/dashboards?project_code=DEFAULT&include_scores=true&include_history=true
    """
    if project_code:
        validate_project_code(project_code, db)
    
    # Determine how many history items to fetch
    history_items = 50 if include_history else 0
    
    definitions = ScoreDefinition.all(
        project_code=project_code,
        name_filter=name_filter,
        sorted_by=sorted_by,
        last_history_items=history_items
    )
    
    # If scores or history are requested, return full dashboard details
    if include_scores or include_history:
        result = []
        for d in definitions:
            # Use fresh score calculation to ensure categories are populated
            # as_cached_score_card() only works after recalculate has been called
            score_card = d.as_score_card()
            
            # If history is requested and available, merge it from cached data
            if include_history and d.history:
                cached_card = d.as_cached_score_card(include_definition=False)
                score_card["history"] = cached_card.get("history", [])
            
            formatted = format_score_card(score_card)
            result.append(DashboardResponse(**formatted))
        return result
    
    # Otherwise, return basic summary
    return [
        DashboardResponse(
            id=str(d.id),
            project_code=d.project_code,
            name=d.name,
            score=None,
            cde_score=None,
            profiling_score=None,
            testing_score=None,
            categories_label=d.category.value if d.category else None,
            categories=[],
            history=[]
        )
        for d in definitions
    ]



@router.get("/dashboards/{dashboard_id}", response_model=DashboardResponse)
def get_dashboard(
    dashboard_id: str,
    include_breakdown: bool = Query(False, description="Include breakdown data"),
    include_history: bool = Query(True, description="Include historical data"),
    db: Session = Depends(get_db_session)
):
    """
    Get detailed information about a specific dashboard.
    
    Args:
        dashboard_id: Dashboard UUID
        include_breakdown: Whether to include breakdown data
        include_history: Whether to include historical data
        db: Database session
        
    Returns:
        Full dashboard details with scores
    """
    dashboard = validate_dashboard_id(dashboard_id, db)
    
    # Get cached score card with optional history
    if include_history:
        # Reload with history
        definitions = ScoreDefinition.all(
            project_code=dashboard.project_code,
            last_history_items=50
        )
        dashboard = next((d for d in definitions if str(d.id) == dashboard_id), dashboard)
    
    score_card = dashboard.as_cached_score_card(include_definition=True)
    formatted = format_score_card(score_card)
    
    return DashboardResponse(**formatted)


@router.get("/dashboards/{dashboard_id}/breakdown", response_model=List[BreakdownItem])
def get_dashboard_breakdown(
    dashboard_id: str,
    score_type: Literal["score", "cde_score"] = Query(..., description="Score type to breakdown"),
    group_by: GroupByCategory = Query(..., description="Category to group by"),
    db: Session = Depends(get_db_session)
):
    """
    Get score breakdown for a dashboard.
    
    Args:
        dashboard_id: Dashboard UUID
        score_type: Type of score to breakdown (score or cde_score)
        group_by: Category to group breakdown by
        db: Database session
        
    Returns:
        List of breakdown items
    """
    dashboard = validate_dashboard_id(dashboard_id, db)
    
    # Convert enum to string value
    group_by_value = group_by.value
    
    # Normalize and validate group_by parameter
    # Map common variations to correct column names (for backwards compatibility)
    group_by_mapping = {
        "table_group": "table_groups_name",
        "table group": "table_groups_name",
        "tablegroup": "table_groups_name",
    }
    
    # Normalize the group_by value
    normalized_group_by = group_by_mapping.get(group_by_value.lower().replace("_", " ").strip(), group_by_value)
    
    # Valid categories from TestGen (already validated by enum, but kept for safety)
    valid_categories = [
        "column_name",
        "table_name",
        "dq_dimension",
        "semantic_data_type",
        "table_groups_name",
        "data_location",
        "data_source",
        "source_system",
        "source_process",
        "business_domain",
        "stakeholder_group",
        "transform_level",
        "data_product",
    ]
    
    if normalized_group_by not in valid_categories:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid group_by parameter: '{group_by}'. Must be one of: {', '.join(valid_categories)}"
        )
    
    # Get breakdown data
    breakdown_data = dashboard.get_score_card_breakdown(score_type, normalized_group_by)
    formatted = format_score_card_breakdown(breakdown_data, normalized_group_by)
    
    return [BreakdownItem(**item) for item in formatted["items"]]


@router.get("/dashboards/{dashboard_id}/issues", response_model=List[IssueItem])
def get_dashboard_issues(
    dashboard_id: str,
    score_type: Literal["score", "cde_score"] = Query(..., description="Score type"),
    group_by: GroupByCategory = Query(..., description="Category grouping"),
    value: str = Query(..., description="Breakdown item value"),
    db: Session = Depends(get_db_session)
):
    """
    Get issues for a specific breakdown item.
    
    Args:
        dashboard_id: Dashboard UUID
        score_type: Type of score
        group_by: Category grouping
        value: Breakdown item value
        db: Database session
        
    Returns:
        List of issues
    """
    dashboard = validate_dashboard_id(dashboard_id, db)
    
    # Convert enum to string value
    group_by_value = group_by.value
    
    # Normalize and validate group_by parameter (same as breakdown endpoint)
    group_by_mapping = {
        "table_group": "table_groups_name",
        "table group": "table_groups_name",
        "tablegroup": "table_groups_name",
    }
    
    normalized_group_by = group_by_mapping.get(group_by_value.lower().replace("_", " ").strip(), group_by_value)
    
    # Valid categories from TestGen (already validated by enum, but kept for safety)
    valid_categories = [
        "column_name",
        "table_name",
        "dq_dimension",
        "semantic_data_type",
        "table_groups_name",
        "data_location",
        "data_source",
        "source_system",
        "source_process",
        "business_domain",
        "stakeholder_group",
        "transform_level",
        "data_product",
    ]
    
    if normalized_group_by not in valid_categories:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid group_by parameter: '{group_by}'. Must be one of: {', '.join(valid_categories)}"
        )
    
    # Get issues data
    issues_data = dashboard.get_score_card_issues(score_type, normalized_group_by, value)
    formatted = format_score_card_issues(issues_data, normalized_group_by)
    
    return [IssueItem(**item) for item in formatted["items"]]


@router.put("/dashboards/{dashboard_id}", response_model=DashboardResponse)
def update_dashboard(
    dashboard_id: str,
    updates: DashboardUpdate,
    db: Session = Depends(get_db_session)
):
    """
    Update a dashboard configuration.
    
    Args:
        dashboard_id: Dashboard UUID
        updates: Fields to update
        db: Database session
        
    Returns:
        Updated dashboard
    """
    dashboard = validate_dashboard_id(dashboard_id, db)
    
    # Update fields
    if updates.name is not None:
        dashboard.name = updates.name
    if updates.total_score is not None:
        dashboard.total_score = updates.total_score
    if updates.cde_score is not None:
        dashboard.cde_score = updates.cde_score
    if updates.category is not None:
        dashboard.category = ScoreCategory[updates.category]
    
    # Update filters if provided
    if updates.filters is not None:
        filters_data = [
            {
                "field": f.field,
                "value": f.value,
                "others": f.others
            }
            for f in updates.filters
        ]
        dashboard.criteria = ScoreDefinitionCriteria.from_filters(
            filters_data,
            group_by_field=updates.group_by_field if updates.group_by_field is not None else True
        )
    
    # Save changes
    dashboard.save()
    
    # Return updated dashboard
    score_card = dashboard.as_cached_score_card(include_definition=True)
    formatted = format_score_card(score_card)
    
    return DashboardResponse(**formatted)


@router.delete("/dashboards/{dashboard_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dashboard(
    dashboard_id: str,
    db: Session = Depends(get_db_session)
):
    """
    Delete a dashboard.
    
    Args:
        dashboard_id: Dashboard UUID
        db: Database session
    """
    dashboard = validate_dashboard_id(dashboard_id, db)
    dashboard.delete()


@router.post("/dashboards/{dashboard_id}/recalculate", response_model=RecalculateResponse)
def recalculate_dashboard(
    dashboard_id: str,
    db: Session = Depends(get_db_session)
):
    """
    Recalculate scores for a dashboard.
    
    Args:
        dashboard_id: Dashboard UUID
        db: Database session
        
    Returns:
        Updated dashboard with fresh scores
    """
    dashboard = validate_dashboard_id(dashboard_id, db)
    
    # Refresh the scores and save them to the database
    # This populates score_definition_results table so the UI can display the dashboard
    # add_history_entry=True adds a history entry for the score graphs
    run_refresh_score_cards_results(definition_id=dashboard_id, add_history_entry=True)
    
    # Get the cached score card (now populated with results and history)
    score_card = dashboard.as_cached_score_card(include_definition=True)
    formatted = format_score_card(score_card)
    
    return RecalculateResponse(
        message="Dashboard scores recalculated successfully",
        dashboard=DashboardResponse(**formatted)
    )

# ============================================================================
# Filter Options Helper Functions
# ============================================================================

def get_filter_field_values(project_code: str, db: Session) -> Dict[str, List[str]]:
    """
    Get all available values for each filter field.
    Based on TestGen's get_score_category_values function.
    
    Note: dq_dimension is NOT included in the database query because it's only
    used for categories/grouping, not for filtering. It has static values.
    """
    values = defaultdict(list)
    
    # DQ dimensions are static and used for categories/grouping, NOT for filtering
    # They are NOT included in filter_values
    
    # Dynamic categories from database (for filtering)
    categories = [
        "table_groups_name",
        "data_location",
        "data_source",
        "source_system",
        "source_process",
        "business_domain",
        "stakeholder_group",
        "transform_level",
        "data_product",
    ]
    
    quote = lambda v: f"'{v}'"
    query = f"""
        SELECT *
        FROM (
            SELECT DISTINCT
                UNNEST(array[{', '.join([quote(c) for c in categories])}]) as category,
                UNNEST(array[{', '.join(categories)}]) AS value
            FROM v_dq_test_scoring_latest_by_column
            WHERE project_code = :project_code
            UNION
            SELECT DISTINCT
                UNNEST(array[{', '.join([quote(c) for c in categories])}]) as category,
                UNNEST(array[{', '.join(categories)}]) AS value
            FROM v_dq_profile_scoring_latest_by_column
            WHERE project_code = :project_code
        ) category_values
        WHERE value IS NOT NULL
        ORDER BY LOWER(value)
    """
    
    results = fetch_all_from_db(query, {"project_code": project_code})
    for row in results:
        if row.category and row.value:
            values[row.category].append(row.value)
    
    return dict(values)


def get_column_hierarchy(project_code: str, db: Session) -> List[ColumnHierarchy]:
    """
    Get hierarchical column data (table groups → tables → columns).
    Based on TestGen's get_column_filters function.
    """
    query = """
    SELECT
        data_column_chars.column_id::text AS column_id,
        data_column_chars.column_name,
        data_column_chars.table_id::text AS table_id,
        data_column_chars.table_name,
        data_column_chars.table_groups_id::text AS table_group_id,
        table_groups.table_groups_name AS table_group_name
    FROM data_column_chars
    INNER JOIN table_groups ON (table_groups.id = data_column_chars.table_groups_id)
    WHERE table_groups.project_code = :project_code
    ORDER BY LOWER(table_groups_name), LOWER(table_name), ordinal_position;
    """
    
    results = fetch_all_from_db(query, {"project_code": project_code})
    return [ColumnHierarchy(**dict(row)) for row in results]


# ============================================================================
# Filter Options Endpoint
# ============================================================================

@router.get("/filter-options", response_model=FilterOptions)
def get_dashboard_filter_options(
    project_code: str = Query(..., description="Project code to get filter options for"),
    include_filter_values: bool = Query(True, description="Include filter field values"),
    include_columns: bool = Query(True, description="Include column hierarchy"),
    include_category_options: bool = Query(True, description="Include category options"),
    include_score_grouping_options: bool = Query(True, description="Include score grouping options"),
    include_score_type_options: bool = Query(True, description="Include score type options"),
    db: Session = Depends(get_db_session)
):
    """
    Get filter options for dashboard creation/editing.
    
    By default, returns all options. Use the include_* parameters to request only specific options.
    
    Args:
        project_code: Project code to get filter options for
        include_filter_values: Include filter field values (default: True)
        include_columns: Include column hierarchy (default: True)
        include_category_options: Include category options (default: True)
        include_score_grouping_options: Include score grouping options (default: True)
        include_score_type_options: Include score type options (default: True)
    
    Returns:
        FilterOptions with requested data
    
    Examples:
        # Get all options (default)
        GET /api/data-quality/filter-options?project_code=DEFAULT
        
        # Get only score grouping and score type options
        GET /api/data-quality/filter-options?project_code=DEFAULT&include_filter_values=false&include_columns=false&include_category_options=false
        
        # Get only filter values and columns
        GET /api/data-quality/filter-options?project_code=DEFAULT&include_category_options=false&include_score_grouping_options=false&include_score_type_options=false
    """
    # Validate project exists
    validate_project_code(project_code, db)
    
    # Initialize response with empty/default values
    filter_fields_metadata = []
    filter_values = {}
    columns = []
    category_options = []
    score_grouping_options = []
    score_type_options = []
    
    # Filter field metadata (labels for UI display)
    if include_filter_values:
        filter_fields_metadata = [
            {"field": "table_groups_name", "label": "Table Group"},
            {"field": "data_location", "label": "Data Location"},
            {"field": "data_source", "label": "Data Source"},
            {"field": "source_system", "label": "Source System"},
            {"field": "source_process", "label": "Source Process"},
            {"field": "business_domain", "label": "Business Domain"},
            {"field": "stakeholder_group", "label": "Stakeholder Group"},
            {"field": "transform_level", "label": "Transform Level"},
            {"field": "data_product", "label": "Data Product"},
        ]
    
    # Get filter field values (only if requested)
    if include_filter_values:
        filter_values = get_filter_field_values(project_code, db)
    
    # Get column hierarchy (only if requested)
    if include_columns:
        columns = get_column_hierarchy(project_code, db)
    
    # Category options (for "Display on scorecard")
    if include_category_options:
        category_options = [
            {"value": "table_groups_name", "label": "Table Group"},
            {"value": "data_location", "label": "Data Location"},
            {"value": "data_source", "label": "Data Source"},
            {"value": "source_system", "label": "Source System"},
            {"value": "source_process", "label": "Source Process"},
            {"value": "business_domain", "label": "Business Domain"},
            {"value": "stakeholder_group", "label": "Stakeholder Group"},
            {"value": "transform_level", "label": "Transform Level"},
            {"value": "dq_dimension", "label": "Quality Dimension"},
            {"value": "data_product", "label": "Data Product"},
        ]
    
    # Score grouping options (for breakdown)
    if include_score_grouping_options:
        score_grouping_options = [
            {"value": "column_name", "label": "Column"},
            {"value": "table_name", "label": "Table"},
            {"value": "dq_dimension", "label": "Quality Dimension"},
            {"value": "semantic_data_type", "label": "Semantic Data Type"},
            {"value": "table_groups_name", "label": "Table Group"},
            {"value": "data_location", "label": "Data Location"},
            {"value": "data_source", "label": "Data Source"},
            {"value": "source_system", "label": "Source System"},
            {"value": "source_process", "label": "Source Process"},
            {"value": "business_domain", "label": "Business Domain"},
            {"value": "stakeholder_group", "label": "Stakeholder Group"},
            {"value": "transform_level", "label": "Transform Level"},
            {"value": "data_product", "label": "Data Product"},
        ]
    
    # Score type options
    if include_score_type_options:
        score_type_options = [
            {"value": "score", "label": "Total Score"},
            {"value": "cde_score", "label": "CDE Score"},
        ]
    
    return FilterOptions(
        filter_fields_metadata=filter_fields_metadata,
        filter_values=filter_values,
        columns=columns,
        category_options=category_options,
        score_grouping_options=score_grouping_options,
        score_type_options=score_type_options
    )
