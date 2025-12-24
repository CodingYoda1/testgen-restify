"""Dependency injection utilities for FastAPI endpoints"""
import sys
from pathlib import Path
from typing import Generator
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

# Add the dataops-testgen directory to Python path
testgen_path = Path(__file__).parent.parent / "dataops-testgen"
if str(testgen_path) not in sys.path:
    sys.path.insert(0, str(testgen_path))

from testgen.common.models import Session as DBSession, get_current_session
from testgen.common.models.scores import ScoreDefinition
from testgen.common.models.project import Project


def get_db_session() -> Generator[Session, None, None]:
    """
    Provides database session for FastAPI endpoints.
    Creates a new session and sets it as the thread-local current session
    so that testgen models can access it via get_current_session().
    """
    from testgen.common.models import _current_session_wrapper
    
    session = DBSession()
    # Set the thread-local session so testgen models can use it
    _current_session_wrapper.value = session
    try:
        yield session
    finally:
        _current_session_wrapper.value = None
        session.close()


def validate_project_code(project_code: str, db: Session) -> None:
    """
    Validates that a project exists.
    
    Args:
        project_code: Project code to validate
        db: Database session
        
    Raises:
        HTTPException: If project doesn't exist
    """
    project = db.query(Project).filter(Project.project_code == project_code).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project '{project_code}' not found"
        )


def validate_dashboard_id(dashboard_id: str, db: Session) -> ScoreDefinition:
    """
    Validates that a dashboard exists and returns it.
    
    Args:
        dashboard_id: Dashboard UUID to validate
        db: Database session
        
    Returns:
        ScoreDefinition: The dashboard definition
        
    Raises:
        HTTPException: If dashboard doesn't exist or invalid UUID
    """
    try:
        uuid_obj = UUID(dashboard_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid dashboard ID format: {dashboard_id}"
        )
    
    dashboard = ScoreDefinition.get(dashboard_id)
    if not dashboard:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard '{dashboard_id}' not found"
        )
    
    return dashboard
