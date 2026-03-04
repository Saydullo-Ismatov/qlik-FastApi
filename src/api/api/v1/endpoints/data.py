from fastapi import APIRouter, Depends, Path, Query, HTTPException
from typing import Optional
from src.api.schemas.data import TableDataResponse, DataFilterParams
from src.api.services.data_service import DataService
from src.api.core.dependencies import get_data_service, verify_api_key
from src.api.core.config import settings


class PaginationData:
    """Simple pagination data holder."""
    def __init__(self, page: int, page_size: int):
        self.page = page
        self.page_size = page_size


router = APIRouter()

@router.get("/apps/{app_name}", response_model=TableDataResponse)
async def get_default_table_data(
    app_name: str = Path(..., description="Application name"),
    page: Optional[int] = Query(None, ge=1, description="Page number (omit for all data)"),
    page_size: int = Query(100, ge=1, le=10000, description="Items per page"),
    all_data: bool = Query(False, description="Set to true to get all data without pagination"),
    filter_field: Optional[str] = Query(None, description="Field to filter on"),
    filter_value: Optional[str] = Query(None, description="Filter value"),
    sort_field: Optional[str] = Query(None, description="Field to sort by"),
    sort_order: str = Query("asc", pattern="^(asc|desc)$", description="Sort order"),
    warehouse: Optional[str] = Query(None, description="Warehouse filter value (e.g., 'Склад 1', '007 Склад')"),
    MeasureType: Optional[str] = Query(None, description="Measure type (1=qty, 2=amount, 3=amount-qty)"),
    Currency: Optional[str] = Query(None, description="Currency type (1=ZUD, 2=UZS, 3=ZUDMVP)"),
    data_service: DataService = Depends(get_data_service),
    api_key: str = Depends(verify_api_key),
    settings: settings = Depends(lambda: settings)
):
    """
    Get paginated data from the default table of a Qlik Sense app.

    **Example:**
    ```
    GET /api/v1/apps/afko?page=1&page_size=50
    ```

    **Get all data (no pagination):**
    ```
    GET /api/v1/apps/afko?all_data=true
    ```

    **With filtering:**
    ```
    GET /api/v1/apps/afko?page=1&page_size=50&filter_field=Department&filter_value=Sales
    ```

    **With sorting:**
    ```
    GET /api/v1/apps/afko?page=1&sort_field=EmployeeID&sort_order=desc
    ```

    **With warehouse filter and variables:**
    ```
    GET /api/v1/apps/afko?page=1&warehouse=Склад 1&MeasureType=1&Currency=2
    ```
    """
    # Check app access
    if not settings.can_access_app(api_key, app_name):
        raise HTTPException(
            status_code=403,
            detail=f"Your API key does not have access to app '{app_name}'"
        )

    # Get the default table ID for this app
    table_id = settings.get_default_table_id(app_name)
    if not table_id:
        raise HTTPException(
            status_code=404,
            detail=f"No default table configured for app '{app_name}'"
        )

    # Check table access
    if not settings.can_access_table(api_key, app_name, table_id):
        raise HTTPException(
            status_code=403,
            detail=f"Your API key does not have access to the default table in app '{app_name}'"
        )

    # Handle all_data flag - use max allowed page size (10000)
    if all_data:
        page = 1
        page_size = 10000  # Qlik's max allowed page size
    elif page is None:
        page = 1  # Default to page 1 if not specified

    pagination = PaginationData(page=page, page_size=page_size)

    # Build selections dictionary - map warehouse parameter to Qlik field "Склад"
    selections = {}
    if warehouse:
        selections["Склад"] = [warehouse]

    # Build variables dictionary - map friendly names to Qlik variable names
    variables = {}
    if MeasureType:
        variables["vChooseType"] = MeasureType
    if Currency:
        variables["vChooseCur"] = Currency

    filters = DataFilterParams(
        filter_field=filter_field,
        filter_value=filter_value,
        sort_field=sort_field,
        sort_order=sort_order,
        selections=selections if selections else None,
        variables=variables if variables else None
    )

    result = await data_service.get_table_data(
        app_name=app_name,
        table_name=table_id,
        pagination=pagination,
        filters=filters
    )

    return result


