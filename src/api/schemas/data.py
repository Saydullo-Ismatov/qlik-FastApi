"""Pydantic schemas for Qlik Sense data operations."""

from typing import Any, Optional

from pydantic import BaseModel, Field
from src.api.schemas.common import PaginationMeta


class DataFilterParams(BaseModel):
    """Parameters for filtering and sorting data."""

    filter_field: Optional[str] = Field(None, description="Field to filter on")
    filter_value: Optional[str] = Field(None, description="Value to filter by")
    sort_field: Optional[str] = Field(None, description="Field to sort by")
    sort_order: str = Field("asc", pattern="^(asc|desc)$", description="Sort order")

    # New parameters for selections and variables
    selections: Optional[dict[str, list[str]]] = Field(
        None,
        description="Field selections to apply (e.g., {'Склад': ['Склад 1', 'Склад 2']})"
    )
    variables: Optional[dict[str, str]] = Field(
        None,
        description="Variable values to set (e.g., {'vChooseType': '1', 'vChooseCur': '2'})"
    )


class TableDataResponse(BaseModel):
    """Response for table data retrieval."""

    app_name: str = Field(..., description="Application name")
    table_name: str = Field(..., description="Table name")
    data: list[dict[str, Any]] = Field(..., description="Table data rows")
    pagination: PaginationMeta = Field(..., description="Pagination metadata")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class HyperCubeDef(BaseModel):
    """Schema for hypercube definition."""

    dimensions: list[dict[str, Any]] = Field(..., description="Dimension definitions")
    measures: list[dict[str, Any]] = Field(..., description="Measure definitions")
    initial_data_fetch: list[dict[str, int]] | None = Field(
        None, alias="qInitialDataFetch", description="Initial data fetch parameters"
    )
    suppress_zero: bool = Field(False, alias="qSuppressZero", description="Suppress zero values")
    suppress_missing: bool = Field(
        False, alias="qSuppressMissing", description="Suppress missing values"
    )


class DataRequest(BaseModel):
    """Request schema for data extraction."""

    hypercube_def: dict[str, Any] = Field(..., description="Hypercube definition")
    page_height: int = Field(default=100, ge=1, le=10000, description="Number of rows per page")
    page_width: int | None = Field(None, description="Number of columns (optional)")


class DataCell(BaseModel):
    """Schema for a data cell."""

    text: str = Field(..., alias="qText", description="Cell text value")
    num: float | None = Field(None, alias="qNum", description="Cell numeric value")
    elem_number: int = Field(..., alias="qElemNumber", description="Element number")
    state: str = Field(..., alias="qState", description="Selection state")


class DataRow(BaseModel):
    """Schema for a data row."""

    cells: list[dict[str, Any]] = Field(..., alias="qValue", description="Row cells")


class DataPage(BaseModel):
    """Schema for a data page."""

    matrix: list[list[dict[str, Any]]] = Field(..., alias="qMatrix", description="Data matrix")
    area: dict[str, Any] = Field(..., alias="qArea", description="Page area")
    row_count: int = Field(..., alias="qArea.qHeight", description="Number of rows")
    col_count: int = Field(..., alias="qArea.qWidth", description="Number of columns")


class HyperCubeDataResponse(BaseModel):
    """Response schema for hypercube data."""

    data: list[list[Any]] = Field(..., description="Data matrix")
    dimensions: list[str] = Field(..., description="Dimension names")
    measures: list[str] = Field(..., description="Measure names")
    total_rows: int = Field(..., description="Total number of rows")
    total_columns: int = Field(..., description="Total number of columns")


class SelectionRequest(BaseModel):
    """Request schema for making selections."""

    field: str = Field(..., description="Field name")
    values: list[str | int] = Field(..., description="Values to select")
    toggle: bool = Field(False, description="Toggle selection instead of replacing")


class SelectionResponse(BaseModel):
    """Response schema for selection operations."""

    success: bool = Field(..., description="Whether selection was successful")
    field: str = Field(..., description="Field name")
    selected_count: int = Field(..., description="Number of values selected")


class FieldValue(BaseModel):
    """Schema for field value."""

    text: str = Field(..., description="Value text")
    elem_number: int = Field(..., description="Element number")
    frequency: int = Field(0, description="Frequency of value")
    state: str = Field(..., description="Selection state")


class FieldValuesResponse(BaseModel):
    """Response schema for field values."""

    field: str = Field(..., description="Field name")
    values: list[FieldValue] = Field(..., description="Field values")
    total: int = Field(..., description="Total number of values")
    cardinal: int = Field(..., description="Field cardinality")


class VariableValue(BaseModel):
    """Schema for variable value."""

    name: str = Field(..., description="Variable name")
    value: str | float | None = Field(..., description="Variable value")
    definition: str | None = Field(None, description="Variable definition/formula")


class VariableListResponse(BaseModel):
    """Response schema for variable list."""

    variables: list[VariableValue] = Field(..., description="List of variables")
    total: int = Field(..., description="Total number of variables")
