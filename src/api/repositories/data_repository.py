"""Repository for Qlik Sense data retrieval operations."""
from typing import Dict, List, Any, Optional
import logging

from src.api.repositories.base import BaseRepository
from src.api.clients.qlik_engine import QlikEngineClient
from src.api.schemas.data import DataFilterParams

logger = logging.getLogger(__name__)


class DataRepository(BaseRepository):
    """Repository for retrieving data from Qlik Sense applications."""

    def __init__(self, engine_client: QlikEngineClient):
        """
        Initialize DataRepository with engine client.

        Args:
            engine_client: Client for Qlik Engine API operations
        """
        self.engine_client = engine_client

    def get_table_data(
        self,
        app_id: str,
        table_name: str,
        page: int,
        page_size: int,
        filters: DataFilterParams
    ) -> Dict[str, Any]:
        """
        Get paginated table data by fetching from a Qlik object (table).

        Args:
            app_id: ID of the application
            table_name: Object/Table ID (e.g., "dvWAj")
            page: Page number (1-indexed)
            page_size: Number of records per page
            filters: Filter parameters including field, value, and sorting

        Returns:
            Dictionary containing:
                - data: List of records
                - total_records: Total number of records
                - metadata: Additional metadata about the query
        """
        try:
            logger.info(
                f"Fetching data from table/object '{table_name}' in app '{app_id}' "
                f"(page={page}, page_size={page_size})"
            )

            # Calculate offset for pagination
            offset = (page - 1) * page_size

            # Track if selections were applied (for cleanup)
            selections_applied = False
            app_handle = -1

            # Connect to engine
            self.engine_client.connect()

            try:
                # Open the app
                app_response = self.engine_client.open_doc(app_id, no_data=False)

                # Extract app handle from response
                app_handle = app_response.get("qReturn", {}).get("qHandle", -1)

                if app_handle == -1:
                    logger.error(f"Failed to get app handle: {app_response}")
                    raise Exception("Failed to open application")

                logger.info(f"Successfully opened app with handle: {app_handle}")

                # Apply variables first (if any)
                if filters.variables:
                    logger.info(f"Setting variables: {filters.variables}")
                    for var_name, var_value in filters.variables.items():
                        try:
                            self.engine_client.set_variable_value(app_handle, var_name, var_value)
                        except Exception as var_error:
                            logger.warning(f"Failed to set variable {var_name}: {var_error}")

                # Apply selections (if any)
                if filters.selections:
                    logger.info(f"Applying selections: {filters.selections}")
                    for field_name, values in filters.selections.items():
                        try:
                            self.engine_client.select_values(app_handle, field_name, values)
                            selections_applied = True
                        except Exception as sel_error:
                            logger.warning(f"Failed to apply selection on {field_name}: {sel_error}")

                # Get the object/table to understand its structure
                try:
                    obj_response = self.engine_client.get_object(app_handle, table_name)
                    logger.info(f"Object response: {obj_response}")
                except Exception as e:
                    logger.warning(f"Could not get object {table_name}: {e}")
                    # If it's not an object ID, treat it as a table name
                    # and get all fields from that table
                    return self._get_data_from_table_fields(
                        app_handle, table_name, offset, page_size, filters
                    )

                # Extract hypercube data from the object
                obj_handle = obj_response.get("qReturn", {}).get("qHandle", -1)

                if obj_handle not in (-1, None):
                    # Get the object's layout to access its hypercube
                    layout_response = self.engine_client.send_request(
                        "GetLayout", {}, handle=obj_handle
                    )

                    layout = layout_response.get("qLayout", {})
                    hypercube = layout.get("qHyperCube", {})

                    if hypercube:
                        # Get the number of columns from hypercube
                        total_cols = hypercube.get("qSize", {}).get("qcx", 10)

                        # Fetch data using GetHyperCubeData method directly
                        logger.info(f"Fetching hypercube data: offset={offset}, page_size={page_size}, cols={total_cols}")

                        # Call GetHyperCubeData with correct parameters
                        data_response = self.engine_client.send_request(
                            "GetHyperCubeData",
                            [
                                "/qHyperCubeDef",
                                [
                                    {
                                        "qTop": offset,
                                        "qLeft": 0,
                                        "qHeight": page_size,
                                        "qWidth": total_cols
                                    }
                                ]
                            ],
                            handle=obj_handle
                        )

                        logger.info(f"Hypercube data response keys: {data_response.keys()}")

                        # Check for errors
                        if "error" in data_response:
                            logger.error(f"Hypercube data error: {data_response.get('error')}")
                            # Return empty data with metadata
                            dim_info = hypercube.get("qDimensionInfo", [])
                            measure_info = hypercube.get("qMeasureInfo", [])
                            columns = []
                            for dim in dim_info:
                                columns.append(dim.get("qFallbackTitle", dim.get("qName", "Dimension")))
                            for measure in measure_info:
                                columns.append(measure.get("qFallbackTitle", measure.get("qName", "Measure")))

                            return {
                                "data": [],
                                "total_records": hypercube.get("qSize", {}).get("qcy", 0),
                                "metadata": {
                                    "table": table_name,
                                    "page": page,
                                    "page_size": page_size,
                                    "columns": columns,
                                    "dimensions": len(dim_info),
                                    "measures": len(measure_info),
                                    "error": str(data_response.get("error"))
                                }
                            }

                        # Extract data pages from response
                        data_pages = data_response.get("qDataPages", [])

                        # Extract matrix data
                        all_rows = []
                        for data_page in data_pages:
                            matrix = data_page.get("qMatrix", [])
                            logger.info(f"Data page has {len(matrix)} rows")
                            all_rows.extend(matrix)

                        # Get dimension and measure info
                        dim_info = hypercube.get("qDimensionInfo", [])
                        measure_info = hypercube.get("qMeasureInfo", [])

                        # Build column names
                        columns = []
                        for dim in dim_info:
                            columns.append(dim.get("qFallbackTitle", dim.get("qName", "Dimension")))
                        for measure in measure_info:
                            columns.append(measure.get("qFallbackTitle", measure.get("qName", "Measure")))

                        # Convert matrix to list of dicts
                        data = []
                        for row in all_rows:
                            row_dict = {}
                            for idx, cell in enumerate(row):
                                col_name = columns[idx] if idx < len(columns) else f"Column_{idx}"
                                # Use qText for display, qNum for numeric value
                                row_dict[col_name] = cell.get("qText", "")
                                if cell.get("qNum") is not None and cell.get("qNum") != "NaN":
                                    row_dict[f"{col_name}_num"] = cell.get("qNum")
                            data.append(row_dict)

                        total_records = hypercube.get("qSize", {}).get("qcy", len(data))

                        return {
                            "data": data,
                            "total_records": total_records,
                            "metadata": {
                                "table": table_name,
                                "page": page,
                                "page_size": page_size,
                                "columns": columns,
                                "dimensions": len(dim_info),
                                "measures": len(measure_info)
                            }
                        }

                # Fallback: get data using table fields
                return self._get_data_from_table_fields(
                    app_handle, table_name, offset, page_size, filters
                )

            finally:
                # Clear selections to keep API stateless
                if selections_applied and app_handle != -1:
                    try:
                        logger.info("Clearing selections to maintain stateless API")
                        self.engine_client.clear_all(app_handle)
                    except Exception as clear_error:
                        logger.warning(f"Failed to clear selections: {clear_error}")

                # Disconnect from engine
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Error fetching table data: {str(e)}", exc_info=True)
            raise


    def _get_data_from_table_fields(
        self,
        app_handle: int,
        table_name: str,
        offset: int,
        page_size: int,
        filters: DataFilterParams
    ) -> Dict[str, Any]:
        """
        Get data from a table by creating a hypercube with all its fields.

        Args:
            app_handle: Application handle
            table_name: Table name
            offset: Starting offset
            page_size: Number of records
            filters: Filter parameters

        Returns:
            Dictionary with data and metadata
        """
        try:
            # Get all fields info
            fields_response = self.engine_client.get_fields(app_handle)
            all_fields = fields_response.get("fields", [])

            # Filter fields for this table
            table_fields = [
                f["field_name"] for f in all_fields
                if f.get("table_name") == table_name
            ]

            if not table_fields:
                logger.warning(f"No fields found for table '{table_name}'")
                return {
                    "data": [],
                    "total_records": 0,
                    "metadata": {
                        "table": table_name,
                        "page": offset // page_size + 1,
                        "page_size": page_size,
                        "fields": []
                    }
                }

            # Create hypercube with table fields as dimensions
            hypercube_result = self.engine_client.create_hypercube(
                app_handle,
                dimensions=table_fields,
                measures=[],
                max_rows=offset + page_size
            )

            if "error" in hypercube_result:
                logger.error(f"Error creating hypercube: {hypercube_result}")
                return {
                    "data": [],
                    "total_records": 0,
                    "metadata": {"error": hypercube_result.get("error")}
                }

            hypercube_data = hypercube_result.get("hypercube_data", {})
            data_pages = hypercube_data.get("qDataPages", [])

            # Extract data
            all_rows = []
            for data_page in data_pages:
                matrix = data_page.get("qMatrix", [])
                # Apply pagination offset
                start_idx = max(0, offset - data_page.get("qArea", {}).get("qTop", 0))
                end_idx = start_idx + page_size
                all_rows.extend(matrix[start_idx:end_idx])

            # Convert to list of dicts
            data = []
            for row in all_rows:
                row_dict = {}
                for idx, cell in enumerate(row):
                    field_name = table_fields[idx] if idx < len(table_fields) else f"Field_{idx}"
                    row_dict[field_name] = cell.get("qText", "")
                    if cell.get("qNum") is not None:
                        row_dict[f"{field_name}_num"] = cell.get("qNum")
                data.append(row_dict)

            total_records = hypercube_result.get("total_rows", len(data))

            return {
                "data": data,
                "total_records": total_records,
                "metadata": {
                    "table": table_name,
                    "page": offset // page_size + 1,
                    "page_size": page_size,
                    "fields": table_fields
                }
            }

        except Exception as e:
            logger.error(f"Error in _get_data_from_table_fields: {str(e)}", exc_info=True)
            raise
