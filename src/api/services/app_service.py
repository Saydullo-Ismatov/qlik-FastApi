"""App service module for application operations."""

import asyncio
from typing import List, Dict

from src.api.services.base import BaseService
from src.api.repositories.app_repository import AppRepository
from src.api.schemas.app import AppMetadata, TableInfo
from src.api.core.exceptions import AppNotFoundException


class AppService(BaseService):
    """Service for app operations.

    Provides business logic for managing Qlik Sense applications,
    including listing apps, retrieving table information, and
    health checking connections.
    """

    def __init__(self, app_repository: AppRepository):
        """Initialize the AppService.

        Args:
            app_repository: Repository for app data access operations.
        """
        self.app_repo = app_repository

    async def list_apps(self) -> List[AppMetadata]:
        """List all available applications.

        Retrieves all configured Qlik Sense applications from the
        repository and returns them as AppMetadata models.

        Returns:
            List of AppMetadata models containing app metadata.

        Raises:
            Exception: If there's an error connecting to Qlik Sense.
        """
        # Execute blocking repository call in thread pool
        apps = await asyncio.to_thread(self.app_repo.list_all_apps)

        # Convert raw dictionaries to Pydantic models
        return [AppMetadata(**app) for app in apps]

    async def list_tables(self, app_name: str) -> List[TableInfo]:
        """List all tables in an application.

        Retrieves all tables/data models within a specific Qlik Sense
        application and returns them as TableInfo models.

        Args:
            app_name: Name of the application to retrieve tables from.

        Returns:
            List of TableInfo models containing table metadata.

        Raises:
            AppNotFoundException: If the specified app doesn't exist.
            Exception: If there's an error connecting to Qlik Sense.
        """
        # Validate app exists and get app ID
        app_id = self.app_repo.get_app_id_by_name(app_name)
        if not app_id:
            raise AppNotFoundException(app_name)

        # Execute blocking repository call in thread pool
        tables = await asyncio.to_thread(self.app_repo.get_app_tables, app_id)

        # Convert raw dictionaries to Pydantic models
        return [TableInfo(**table) for table in tables]

    async def check_connection(self) -> bool:
        """Check Qlik connection health.

        Tests the connection to Qlik Sense server to ensure it's
        accessible and responding properly.

        Returns:
            True if connection is healthy, False otherwise.
        """
        # Execute blocking repository call in thread pool
        return await asyncio.to_thread(self.app_repo.check_connection)

    async def get_object_definition(self, app_name: str, object_id: str) -> Dict:
        """Get dimensions and measures from an object.

        Retrieves the full definition of dimensions and measures from
        a Qlik Sense object (pivot table, chart, etc.).

        Args:
            app_name: Name of the application containing the object.
            object_id: ID of the object to retrieve definition from.

        Returns:
            Dictionary containing object definition with dimensions and measures.

        Raises:
            AppNotFoundException: If the specified app doesn't exist.
            Exception: If there's an error retrieving the object.
        """
        # Validate app exists and get app ID
        app_id = self.app_repo.get_app_id_by_name(app_name)
        if not app_id:
            raise AppNotFoundException(app_name)

        # Execute blocking repository call in thread pool
        definition = await asyncio.to_thread(
            self.app_repo.get_object_definition,
            app_id,
            object_id
        )

        # Add app_name to the result
        definition['app_name'] = app_name

        return definition

    async def get_pivot_object_data(self, app_name: str, object_id: str, page: int = 1, page_size: int = 100, selections: Dict = None, bookmark_id: str = None) -> Dict:
        """Get data from a pivot-table object using GetHyperCubePivotData.

        Much faster than session hypercube creation for pivot table objects.

        When bookmark_id is provided it is applied to the Qlik session before
        fetching, which filters the pivot table and reveals all dimension levels
        that would otherwise be too large to compute.

        Args:
            app_name: Name of the application.
            object_id: ID of the pivot table object.
            page: Page number (1-based).
            page_size: Number of rows per page.
            selections: Optional dict of field selections (field -> [values]).
            bookmark_id: Optional bookmark ID to apply before fetching.

        Returns:
            Dictionary containing data rows with pagination info.
        """
        app_id = self.app_repo.get_app_id_by_name(app_name)
        if not app_id:
            raise AppNotFoundException(app_name)

        try:
            # Add 10 second timeout using asyncio.wait_for
            data = await asyncio.wait_for(
                asyncio.to_thread(
                    self.app_repo.get_pivot_object_data,
                    app_id,
                    object_id,
                    page,
                    page_size,
                    selections or {},
                    bookmark_id
                ),
                timeout=10.0
            )

            data['app_name'] = app_name
            return data
        except asyncio.TimeoutError:
            raise Exception(f"Request timed out after 10 seconds - pivot table computation is taking too long. Consider using a bookmark to pre-filter the data.")

    async def get_object_data(self, app_name: str, object_id: str, page: int = 1, page_size: int = 100, filters: Dict = None, selections: Dict = None, variables: Dict = None, bookmark_id: str = None) -> Dict:
        """Get actual data from an object.

        Retrieves data rows with dimension and measure values from
        a Qlik Sense object (pivot table, chart, etc.).

        Args:
            app_name: Name of the application containing the object.
            object_id: ID of the object to retrieve data from.
            page: Page number (1-based).
            page_size: Number of rows per page.
            filters: Optional dictionary of field filters for client-side filtering (field_name: value).
            selections: Optional dictionary of field selections to apply in Qlik (field_name: [values]).
            variables: Optional dictionary of variable values to set in Qlik (var_name: value).
            bookmark_id: Optional bookmark ID to apply before fetching data.

        Returns:
            Dictionary containing data rows with pagination info.

        Raises:
            AppNotFoundException: If the specified app doesn't exist.
            Exception: If there's an error retrieving the data.
        """
        # Validate app exists and get app ID
        app_id = self.app_repo.get_app_id_by_name(app_name)
        if not app_id:
            raise AppNotFoundException(app_name)

        # Execute blocking repository call in thread pool
        data = await asyncio.to_thread(
            self.app_repo.get_object_data,
            app_id,
            object_id,
            page,
            page_size,
            filters or {},
            selections or {},
            variables or {},
            bookmark_id
        )

        # Add app_name to the result
        data['app_name'] = app_name

        return data
