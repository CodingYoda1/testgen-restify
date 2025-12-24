from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class DashboardFilterCreate(BaseModel):
    """Filter definition for dashboard creation"""
    field: str = Field(..., description="Field name to filter on")
    value: str = Field(..., description="Value to filter by")
    others: list[dict[str, str]] = Field(default_factory=list, description="Linked filters")


class DashboardCreate(BaseModel):
    """Request schema for creating a new dashboard"""
    name: str = Field(..., min_length=1, max_length=255, description="Dashboard name")
    project_code: str = Field(..., description="Project code this dashboard belongs to")
    total_score: bool = Field(default=True, description="Calculate total score")
    cde_score: bool = Field(default=False, description="Calculate CDE score")
    category: Optional[Literal[
        "table_groups_name",
        "data_location",
        "data_source",
        "source_system",
        "source_process",
        "business_domain",
        "stakeholder_group",
        "transform_level",
        "dq_dimension",
        "data_product"
    ]] = Field(None, description="Category to group scores by")
    filters: list[DashboardFilterCreate] = Field(default_factory=list, description="Filter criteria")
    group_by_field: bool = Field(default=True, description="Group filters by field name")


class DashboardUpdate(BaseModel):
    """Request schema for updating a dashboard"""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Dashboard name")
    total_score: Optional[bool] = Field(None, description="Calculate total score")
    cde_score: Optional[bool] = Field(None, description="Calculate CDE score")
    category: Optional[Literal[
        "table_groups_name",
        "data_location",
        "data_source",
        "source_system",
        "source_process",
        "business_domain",
        "stakeholder_group",
        "transform_level",
        "dq_dimension",
        "data_product"
    ]] = Field(None, description="Category to group scores by")
    filters: Optional[list[DashboardFilterCreate]] = Field(None, description="Filter criteria")
    group_by_field: Optional[bool] = Field(None, description="Group filters by field name")


class CategoryScore(BaseModel):
    """Score for a specific category"""
    label: str = Field(..., description="Category label")
    score: Optional[str] = Field(None, description="Formatted score value")


class HistoryEntry(BaseModel):
    """Historical score entry"""
    score: float = Field(..., description="Score value (0-100)")
    category: Literal["score", "cde_score"] = Field(..., description="Score type")
    time: str = Field(..., description="Timestamp in ISO format")


class DashboardResponse(BaseModel):
    """Full dashboard response with scores"""
    id: str = Field(..., description="Dashboard UUID")
    project_code: str = Field(..., description="Project code")
    name: str = Field(..., description="Dashboard name")
    score: Optional[str] = Field(None, description="Overall score")
    cde_score: Optional[str] = Field(None, description="CDE score")
    profiling_score: Optional[str] = Field(None, description="Profiling score")
    testing_score: Optional[str] = Field(None, description="Testing score")
    categories_label: Optional[str] = Field(None, description="Label for category grouping")
    categories: list[CategoryScore] = Field(default_factory=list, description="Category scores")
    history: list[HistoryEntry] = Field(default_factory=list, description="Historical scores")


class DashboardSummary(BaseModel):
    """Lightweight dashboard listing"""
    id: str = Field(..., description="Dashboard UUID")
    project_code: str = Field(..., description="Project code")
    name: str = Field(..., description="Dashboard name")
    total_score: bool = Field(..., description="Has total score")
    cde_score: bool = Field(..., description="Has CDE score")
    category: Optional[str] = Field(None, description="Category grouping")


class BreakdownItem(BaseModel):
    """Score breakdown item"""
    impact: str = Field(..., description="Impact percentage")
    score: str = Field(..., description="Score value")
    issue_ct: int = Field(..., description="Number of issues")
    # Dynamic fields based on category
    table_groups_id: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    dq_dimension: Optional[str] = None
    semantic_data_type: Optional[str] = None
    table_groups_name: Optional[str] = None
    data_location: Optional[str] = None
    data_source: Optional[str] = None
    source_system: Optional[str] = None
    source_process: Optional[str] = None
    business_domain: Optional[str] = None
    stakeholder_group: Optional[str] = None
    transform_level: Optional[str] = None
    data_product: Optional[str] = None

    class Config:
        extra = "allow"


class IssueItem(BaseModel):
    """Individual issue (hygiene or test)"""
    type: str = Field(..., description="Issue type")
    status: str = Field(..., description="Issue status")
    detail: str = Field(..., description="Issue details")
    time: int = Field(..., description="Timestamp")
    column: Optional[str] = Field(None, description="Column name if applicable")

    class Config:
        extra = "allow"


class RecalculateResponse(BaseModel):
    """Response for recalculate operation"""
    message: str = Field(..., description="Success message")
    dashboard: DashboardResponse = Field(..., description="Updated dashboard")
