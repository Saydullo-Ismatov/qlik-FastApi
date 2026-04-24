"""Repository for Qlik Sense application operations."""
from typing import List, Dict, Optional
import logging

from src.api.repositories.base import BaseRepository
from src.api.clients.qlik_repository import QlikRepositoryClient
from src.api.clients.qlik_engine import QlikEngineClient
from src.api.core.config import settings

logger = logging.getLogger(__name__)


class AppRepository(BaseRepository):
    """Repository for managing Qlik Sense applications."""

    def __init__(
        self,
        repository_client: QlikRepositoryClient,
        engine_client: QlikEngineClient
    ):
        """
        Initialize AppRepository with required clients.

        Args:
            repository_client: Client for Qlik Repository API operations
            engine_client: Client for Qlik Engine API operations
        """
        self.repository_client = repository_client
        self.engine_client = engine_client

    def get_app_id_by_name(self, app_name: str) -> Optional[str]:
        """
        Get application ID from application name using settings.app_mappings.

        Args:
            app_name: Name of the application

        Returns:
            Application ID if found, None otherwise
        """
        try:
            app_id = settings.get_app_id(app_name)
            if app_id:
                logger.info(f"Found app ID '{app_id}' for app name '{app_name}'")
            else:
                logger.warning(f"No app ID found for app name '{app_name}'")
            return app_id
        except Exception as e:
            logger.error(f"Error getting app ID for '{app_name}': {str(e)}")
            return None

    def get_app_metadata(self, app_id: str) -> Dict:
        """
        Get application metadata from Repository API.

        Args:
            app_id: ID of the application

        Returns:
            Dictionary containing app metadata
        """
        try:
            logger.info(f"Fetching metadata for app ID '{app_id}'")
            app_data = self.repository_client.get_app_by_id(app_id)

            # Extract relevant metadata
            metadata = {
                "id": app_data.get("id", app_id),
                "name": app_data.get("name", ""),
                "description": app_data.get("description", ""),
                "published": app_data.get("published", False),
                "stream": app_data.get("stream", {}),
                "owner": app_data.get("owner", {}),
                "created_date": app_data.get("createdDate", ""),
                "modified_date": app_data.get("modifiedDate", ""),
                "file_size": app_data.get("fileSize", 0)
            }

            logger.info(f"Successfully retrieved metadata for app '{metadata.get('name')}'")
            return metadata
        except Exception as e:
            logger.error(f"Error fetching app metadata for '{app_id}': {str(e)}")
            raise

    def list_all_apps(self) -> List[Dict]:
        """
        List all available applications from settings.app_mappings.

        Returns:
            List of dictionaries containing app information
        """
        try:
            app_mappings = settings.app_mappings or {}
            apps = []

            for app_name, app_id in app_mappings.items():
                apps.append({
                    "qDocId": app_id,
                    "qDocName": app_name,
                    "qTitle": app_name,
                    "qThumbnail": None,
                    "qLastReloadTime": None,
                    "qModifiedDate": None,
                    "qFileSize": None,
                    "published": False,
                    "stream_name": None
                })

            logger.info(f"Found {len(apps)} apps in configuration")
            return apps
        except Exception as e:
            logger.error(f"Error listing apps: {str(e)}")
            return []

    def get_app_fields(self, app_id: str) -> List[Dict]:
        """
        Get all fields in an application.

        Args:
            app_id: ID of the application

        Returns:
            List of dictionaries containing field information
        """
        try:
            logger.info(f"Fetching fields for app ID '{app_id}'")

            # Connect to engine
            self.engine_client.connect()

            try:
                # Open the app
                app = self.engine_client.open_doc(app_id)

                # Get field list
                field_list = app.GetFieldList()

                fields = []
                for field_info in field_list:
                    fields.append({
                        "name": field_info.get("qName", ""),
                        "src_tables": field_info.get("qSrcTables", []),
                        "is_system": field_info.get("qIsSystem", False),
                        "is_hidden": field_info.get("qIsHidden", False),
                        "is_semantic": field_info.get("qIsSemantic", False),
                        "distinct_count": field_info.get("qCardinal", 0),
                        "total_count": field_info.get("qTotalCount", 0),
                        "tags": field_info.get("qTags", [])
                    })

                logger.info(f"Found {len(fields)} fields in app '{app_id}'")
                return fields
            finally:
                # Always disconnect
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Error fetching fields for app '{app_id}': {str(e)}")
            raise

    def get_app_tables(self, app_id: str) -> List[str]:
        """
        Get all table names in an application.

        Args:
            app_id: ID of the application

        Returns:
            List of table names
        """
        try:
            logger.info(f"Fetching tables for app ID '{app_id}'")

            # Connect to engine
            self.engine_client.connect()

            try:
                # Open the app
                app = self.engine_client.open_doc(app_id)

                # Get table list
                table_list = app.GetTableList()

                tables = []
                for table_info in table_list:
                    table_name = table_info.get("qName", "")
                    if table_name:
                        tables.append(table_name)

                logger.info(f"Found {len(tables)} tables in app '{app_id}'")
                return tables
            finally:
                # Always disconnect
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Error fetching tables for app '{app_id}': {str(e)}")
            raise

    def check_connection(self) -> bool:
        """
        Test Qlik Sense connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            logger.info("Testing Qlik Sense connection")

            # Try to connect to engine
            self.engine_client.connect()

            try:
                # Try to get engine version to verify connection
                version = self.engine_client.get_engine_version()
                logger.info(f"Successfully connected to Qlik Engine version: {version}")
                return True
            finally:
                # Disconnect
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def get_object_definition(self, app_id: str, object_id: str) -> Dict:
        """
        Get dimensions and measures from a Qlik object (chart, table, etc.).

        Args:
            app_id: ID of the application
            object_id: ID of the object

        Returns:
            Dictionary containing dimensions and measures with their definitions
        """
        try:
            logger.info(f"Fetching object definition for '{object_id}' in app '{app_id}'")

            # Connect to engine
            self.engine_client.connect()

            try:
                # Open the app
                result = self.engine_client.open_doc(app_id, no_data=False)
                app_handle = result['qReturn']['qHandle']

                # Get the object
                obj_response = self.engine_client.send_request('GetObject', [object_id], handle=app_handle)
                obj_handle = obj_response['qReturn']['qHandle']

                # Get object info
                obj_info_response = self.engine_client.send_request('GetInfo', handle=obj_handle)
                obj_info = obj_info_response.get('qInfo', {})
                obj_type = obj_info.get('qType', 'unknown')

                # Get properties with full definitions
                properties = self.engine_client.send_request('GetProperties', handle=obj_handle)
                props = properties.get('qProp', {})
                hc_def = props.get('qHyperCubeDef', {})

                # Extract dimensions
                dimensions = []
                for dim_def in hc_def.get('qDimensions', []):
                    field_defs = dim_def.get('qDef', {}).get('qFieldDefs', [])
                    field_labels = dim_def.get('qDef', {}).get('qFieldLabels', [])

                    field = field_defs[0] if field_defs else None
                    label = field_labels[0] if field_labels else None

                    if field:
                        dimensions.append({
                            'field': field,
                            'label': label if label else field
                        })

                # Extract measures
                measures = []
                for measure_def in hc_def.get('qMeasures', []):
                    expression = measure_def.get('qDef', {}).get('qDef', '')
                    label = measure_def.get('qDef', {}).get('qLabel', '')
                    num_format = measure_def.get('qDef', {}).get('qNumFormat')

                    if expression:
                        measures.append({
                            'expression': expression,
                            'label': label if label else expression,
                            'number_format': num_format
                        })

                logger.info(f"Found {len(dimensions)} dimensions and {len(measures)} measures in object '{object_id}'")

                return {
                    'object_id': object_id,
                    'object_type': obj_type,
                    'app_id': app_id,
                    'dimensions': dimensions,
                    'measures': measures
                }

            finally:
                # Always disconnect
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Error fetching object definition for '{object_id}': {str(e)}")
            raise

    def get_pivot_object_data(self, app_id: str, object_id: str, page: int = 1, page_size: int = 100, selections: Dict = None, bookmark_id: str = None) -> Dict:
        """
        Get data from a pivot-table object using GetHyperCubePivotData.

        This is much faster than creating a session hypercube for pivot objects
        because it reads from the already-computed Qlik pivot table.

        When a bookmark_id is supplied, it is applied to the app session before
        fetching data so the pivot reflects the bookmarked filter state. This is
        critical for tables with many dimensions where an unfiltered pivot would
        be too large to compute.

        Args:
            app_id: ID of the application
            object_id: ID of the pivot table object
            page: Page number (1-based)
            page_size: Number of rows per page
            selections: Optional dict of field selections
            bookmark_id: Optional bookmark ID to apply before fetching

        Returns:
            Dictionary containing data rows with pagination
        """
        try:
            logger.info(f"Fetching pivot data from object '{object_id}' in app '{app_id}'")
            if bookmark_id:
                logger.info(f"Will apply bookmark '{bookmark_id}' before fetching")

            self.engine_client.connect()

            try:
                result = self.engine_client.open_doc(app_id, no_data=False)
                app_handle = result['qReturn']['qHandle']

                pivot_data = self.engine_client.get_pivot_data(
                    app_handle=app_handle,
                    object_id=object_id,
                    page=page,
                    page_size=page_size,
                    selections=selections or {},
                    bookmark_id=bookmark_id
                )

                if 'error' in pivot_data:
                    logger.error(f"Pivot data error details: {pivot_data}")
                    raise Exception(f"Pivot data error: {pivot_data['error']}")

                return pivot_data

            finally:
                try:
                    self.engine_client.disconnect()
                except Exception as disc_err:
                    logger.warning(f"Error during disconnect: {disc_err}")

        except Exception as e:
            logger.error(f"Error fetching pivot data from '{object_id}': {str(e)}", exc_info=True)
            raise

    def get_object_data(self, app_id: str, object_id: str, page: int = 1, page_size: int = 100, filters: Dict = None, selections: Dict = None, variables: Dict = None, bookmark_id: str = None) -> Dict:
        """
        Get actual data from a Qlik object with dimensions and measures.

        Args:
            app_id: ID of the application
            object_id: ID of the object
            page: Page number (1-based)
            page_size: Number of rows per page
            filters: Optional dictionary of field filters for client-side filtering (field_name: value)
            selections: Optional dictionary of field selections to apply in Qlik before retrieving data (field_name: [values])
            variables: Optional dictionary of variable values to set in Qlik before retrieving data (var_name: value)
            bookmark_id: Optional bookmark ID to apply before fetching data

        Returns:
            Dictionary containing data rows with dimension and measure values
        """
        try:
            logger.info(f"Fetching data from object '{object_id}' in app '{app_id}'")
            if bookmark_id:
                logger.info(f"Will apply bookmark '{bookmark_id}' before fetching")
            if filters:
                logger.info(f"Will apply client-side filters: {filters}")
            if selections:
                logger.info(f"Will apply Qlik selections: {selections}")
            if variables:
                logger.info(f"Will set Qlik variables: {variables}")

            # Connect to engine
            self.engine_client.connect()

            try:
                # Open the app
                result = self.engine_client.open_doc(app_id, no_data=False)
                app_handle = result['qReturn']['qHandle']

                # Apply bookmark first if provided
                if bookmark_id:
                    self.engine_client.apply_bookmark(app_handle, bookmark_id)

                # Set Qlik variables if provided (must be done before selections)
                if variables:
                    for var_name, var_value in variables.items():
                        logger.info(f"Setting variable '{var_name}' to '{var_value}'")
                        try:
                            self.engine_client.set_variable_value(app_handle, var_name, var_value)
                        except Exception as var_error:
                            logger.warning(f"Failed to set variable '{var_name}': {str(var_error)}")

                # Apply Qlik selections if provided
                if selections:
                    for field_name, field_values in selections.items():
                        # Ensure field_values is a list
                        if not isinstance(field_values, list):
                            field_values = [field_values]

                        logger.info(f"Applying selection on field '{field_name}' with values: {field_values}")
                        try:
                            self.engine_client.select_values(app_handle, field_name, field_values)
                        except Exception as sel_error:
                            logger.warning(f"Failed to apply selection on '{field_name}': {str(sel_error)}")

                # Get the object
                obj_response = self.engine_client.send_request('GetObject', [object_id], handle=app_handle)
                obj_handle = obj_response['qReturn']['qHandle']

                # Get layout first to get correct dimension/measure info and row count
                layout = self.engine_client.send_request('GetLayout', [], handle=obj_handle)
                hc_layout = layout.get('qLayout', {}).get('qHyperCube', {})
                qsize = hc_layout.get('qSize', {})
                total_rows = qsize.get('qcy', 0)
                visible_cols = qsize.get('qcx', 0)

                logger.info(f"Hypercube size from layout: {qsize}")
                logger.info(f"Total rows reported: {total_rows}")
                dim_count = len(hc_layout.get('qDimensionInfo', []))
                meas_count = len(hc_layout.get('qMeasureInfo', []))
                logger.info(f"Dimension count: {dim_count}")
                logger.info(f"Measure count: {meas_count}")

                # Detect pivot tables that need expansion:
                # 1. If total rows is 0 but dimensions exist (calc condition unfulfilled)
                # 2. If visible columns (qcx) is less than dimensions (pivot is collapsed)
                # In both cases, use GetHyperCubePivotData which returns expanded data
                is_collapsed_pivot = visible_cols < dim_count and dim_count > 0
                is_empty_pivot = total_rows == 0 and dim_count > 0
                logger.info(f"Pivot detection: visible_cols={visible_cols}, dim_count={dim_count}, is_collapsed_pivot={is_collapsed_pivot}, is_empty_pivot={is_empty_pivot}")

                use_regular_hypercube = False  # Flag to control flow

                if is_collapsed_pivot or is_empty_pivot:
                    if is_collapsed_pivot:
                        logger.info(f"Pivot table has {dim_count} dimensions but only {visible_cols} visible columns - will try to get expanded data")
                    else:
                        logger.info("Hypercube has 0 rows but has dimensions - trying GetHyperCubePivotData for pivot table")

                    # Apply selections if provided (without bookmark)
                    if selections:
                        for field_name, field_values in selections.items():
                            if not isinstance(field_values, list):
                                field_values = [field_values]
                            logger.info(f"Applying selection on field '{field_name}' with values: {field_values}")
                            try:
                                self.engine_client.select_values(app_handle, field_name, field_values)
                            except Exception as sel_error:
                                logger.warning(f"Failed to apply selection on '{field_name}': {str(sel_error)}")

                    # Try to get pivot data directly without bookmark
                    pivot_data = self.engine_client.get_pivot_data(
                        app_handle=app_handle,
                        object_id=object_id,
                        page=page,
                        page_size=page_size,
                        selections={},  # Already applied above
                        bookmark_id=None  # NO bookmark!
                    )

                    should_fallback = False

                    if 'error' in pivot_data:
                        logger.warning(f"Pivot data fetch failed: {pivot_data['error']} - falling back to regular hypercube")
                        should_fallback = True
                    else:
                        # Check if pivot data has all expected dimensions
                        # If it's collapsed and returning sparse format, it may only have 1 dimension
                        returned_data = pivot_data.get('data', [])
                        if returned_data:
                            first_row_keys = set(returned_data[0].keys())
                            # Count how many dimension labels are present in the data
                            dim_labels_in_data = sum(1 for dim_info in hc_layout.get('qDimensionInfo', [])
                                                    if dim_info.get('qFallbackTitle', '') in first_row_keys)

                            logger.info(f"Pivot data returned {dim_labels_in_data} dimensions out of {dim_count} expected")

                            if dim_labels_in_data >= dim_count:
                                # All dimensions present, clear selections and return pivot data
                                if selections:
                                    try:
                                        self.engine_client.clear_all(app_handle)
                                    except Exception:
                                        pass
                                pivot_data['app_id'] = app_id
                                return pivot_data
                            else:
                                logger.info(f"Pivot data is incomplete ({dim_labels_in_data}/{dim_count} dimensions) - falling back to regular hypercube for full expansion")
                                should_fallback = True
                        else:
                            # Empty data, clear selections and return pivot result as-is
                            if selections:
                                try:
                                    self.engine_client.clear_all(app_handle)
                                except Exception:
                                    pass
                            pivot_data['app_id'] = app_id
                            return pivot_data

                    if should_fallback:
                        # DON'T clear selections - we need them for the session hypercube
                        logger.info("Creating session object with flat table to get all dimensions")

                        # Extract dimension field definitions from the pivot object
                        dim_defs = []
                        dim_labels_list = []
                        for dim_info in hc_layout.get('qDimensionInfo', []):
                            label = dim_info.get('qFallbackTitle', '')
                            group_defs = dim_info.get('qGroupFieldDefs', [])
                            field = group_defs[0] if group_defs else label

                            if field:  # Skip empty fields
                                dim_defs.append(field)
                                dim_labels_list.append(label)

                        # Extract measure definitions from the pivot object
                        meas_defs = []
                        meas_labels_list = []

                        # Get the full property tree to access measure expressions
                        properties = self.engine_client.send_request('GetFullPropertyTree', handle=obj_handle)
                        prop_entry = properties.get('qPropEntry', {})
                        prop_property = prop_entry.get('qProperty', {})
                        hc_def = prop_property.get('qHyperCubeDef', {})

                        logger.info(f"Found {len(hc_def.get('qMeasures', []))} measure definitions in pivot")

                        for i, meas_def in enumerate(hc_def.get('qMeasures', [])):
                            qdef = meas_def.get('qDef', {})
                            expression = qdef.get('qDef', '')
                            label = qdef.get('qLabel', '')
                            library_id = meas_def.get('qLibraryId', '')

                            logger.info(f"Measure {i}: expression='{expression}', label='{label}', library_id='{library_id}'")

                            # Store the measure definition (either library ID or expression)
                            if library_id or expression:
                                # We'll pass the entire measure definition to preserve library references
                                meas_defs.append(meas_def)
                                meas_labels_list.append(label if label else f"Measure_{i}")
                                logger.info(f"Measure {i} added: library_id={library_id}, has_expression={bool(expression)}")

                        # Get measure labels from layout (more reliable)
                        for i, meas_info in enumerate(hc_layout.get('qMeasureInfo', [])):
                            label = meas_info.get('qFallbackTitle', '')
                            if label:
                                # Add measure if not yet added
                                if i >= len(meas_labels_list):
                                    # Get expression from qMeasureInfo if available
                                    expr = meas_info.get('qFallbackTitle', f'Measure_{i}')
                                    meas_defs.append(f"[{label}]")  # Use field reference
                                    meas_labels_list.append(label)
                                else:
                                    meas_labels_list[i] = label

                        logger.info(f"Creating session hypercube with {len(dim_defs)} dimensions and {len(meas_defs)} measures")
                        logger.info(f"Dimensions: {dim_labels_list}")
                        logger.info(f"Measures: {meas_labels_list}")

                        # Create a session object with flat table hypercube
                        session_obj_def = {
                            "qInfo": {
                                "qType": "table"
                            },
                            "qHyperCubeDef": {
                                "qDimensions": [{"qDef": {"qFieldDefs": [field]}} for field in dim_defs],
                                "qMeasures": meas_defs,  # Use full measure definitions (preserves library IDs)
                                "qMode": "S",  # S = Straight table (not P = Pivot)
                                "qSuppressZero": False,
                                "qSuppressMissing": False,
                                "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": 0, "qWidth": 0}]
                            }
                        }

                        # Create session object
                        session_obj = self.engine_client.send_request(
                            'CreateSessionObject',
                            [session_obj_def],
                            handle=app_handle
                        )
                        session_handle = session_obj['qReturn']['qHandle']

                        try:
                            # Get layout to check total rows
                            session_layout = self.engine_client.send_request('GetLayout', [], handle=session_handle)
                            session_hc = session_layout.get('qLayout', {}).get('qHyperCube', {})
                            session_total_rows = session_hc.get('qSize', {}).get('qcy', 0)

                            logger.info(f"Session hypercube has {session_total_rows} rows")

                            # Determine if we need client-side filtering
                            need_client_side_filtering = bool(filters) or bool(selections)

                            session_data = []
                            total_cols = len(dim_defs) + len(meas_defs)

                            if need_client_side_filtering:
                                # Fetch ALL rows in batches for client-side filtering
                                logger.info(f"Client-side filtering needed - fetching all {session_total_rows} rows in batches")
                                # Calculate optimal chunk size based on column count
                                # Qlik has a cell limit per request - typically around 10,000 cells
                                # Adjust chunk size dynamically to maximize throughput while staying safe
                                MAX_CELLS = 10000  # Conservative limit based on Qlik Engine constraints
                                CHUNK_SIZE = min(2000, MAX_CELLS // max(total_cols, 1))  # Cap at 2000 rows max
                                logger.info(f"Calculated chunk size: {CHUNK_SIZE} rows (columns: {total_cols}, max cells: {MAX_CELLS})")
                                current_row = 0

                                while current_row < session_total_rows:
                                    fetch_rows = min(CHUNK_SIZE, session_total_rows - current_row)
                                    logger.info(f"Fetching batch: rows {current_row} to {current_row + fetch_rows - 1}")

                                    data_request = self.engine_client.send_request(
                                        'GetHyperCubeData',
                                        ['/qHyperCubeDef', [{'qTop': current_row, 'qLeft': 0, 'qWidth': total_cols, 'qHeight': fetch_rows}]],
                                        handle=session_handle
                                    )
                                    data_pages = data_request.get('qDataPages', [])
                                    if data_pages:
                                        matrix = data_pages[0].get('qMatrix', [])
                                        logger.info(f"Batch returned {len(matrix)} rows with {len(matrix[0]) if matrix else 0} columns")

                                        for row_idx, row in enumerate(matrix):
                                            row_data = {}
                                            # Process dimensions
                                            for col_idx, cell in enumerate(row):
                                                if col_idx < len(dim_labels_list):
                                                    # Dimension: use qText
                                                    label = dim_labels_list[col_idx]
                                                    value = cell.get('qText', '')
                                                    row_data[label] = value
                                                elif col_idx < len(dim_labels_list) + len(meas_labels_list):
                                                    # Measure: prefer qNum, fallback to qText
                                                    meas_idx = col_idx - len(dim_labels_list)
                                                    label = meas_labels_list[meas_idx]
                                                    num_val = cell.get('qNum')
                                                    if num_val is not None and str(num_val).lower() not in ('nan', 'inf', '-inf'):
                                                        row_data[label] = num_val
                                                    else:
                                                        row_data[label] = cell.get('qText', '')
                                            if current_row == 0 and row_idx == 0:  # Log first row for debugging
                                                logger.info(f"First session row data: {row_data}")
                                            session_data.append(row_data)

                                    current_row += fetch_rows

                                logger.info(f"Fetched total of {len(session_data)} rows for client-side filtering")
                            else:
                                # No filtering needed - fetch only requested page
                                start_row = (page - 1) * page_size
                                fetch_rows = min(page_size, session_total_rows - start_row) if start_row < session_total_rows else 0

                                if fetch_rows > 0:
                                    logger.info(f"Fetching {fetch_rows} rows from session hypercube starting at row {start_row}")
                                    data_request = self.engine_client.send_request(
                                        'GetHyperCubeData',
                                        ['/qHyperCubeDef', [{'qTop': start_row, 'qLeft': 0, 'qWidth': total_cols, 'qHeight': fetch_rows}]],
                                        handle=session_handle
                                    )
                                    data_pages = data_request.get('qDataPages', [])
                                    if data_pages:
                                        matrix = data_pages[0].get('qMatrix', [])
                                        logger.info(f"Session qMatrix returned {len(matrix)} rows with {len(matrix[0]) if matrix else 0} columns")
                                        if matrix and len(matrix) > 0:
                                            logger.info(f"First row sample: {matrix[0][:3]}")  # Log first 3 cells
                                        for row_idx, row in enumerate(matrix):
                                            row_data = {}
                                            # Process dimensions
                                            for col_idx, cell in enumerate(row):
                                                if col_idx < len(dim_labels_list):
                                                    # Dimension: use qText
                                                    label = dim_labels_list[col_idx]
                                                    value = cell.get('qText', '')
                                                    row_data[label] = value
                                                elif col_idx < len(dim_labels_list) + len(meas_labels_list):
                                                    # Measure: prefer qNum, fallback to qText
                                                    meas_idx = col_idx - len(dim_labels_list)
                                                    label = meas_labels_list[meas_idx]
                                                    num_val = cell.get('qNum')
                                                    if num_val is not None and str(num_val).lower() not in ('nan', 'inf', '-inf'):
                                                        row_data[label] = num_val
                                                    else:
                                                        row_data[label] = cell.get('qText', '')
                                            if row_idx == 0:  # Log first row for debugging
                                                logger.info(f"First session row data: {row_data}")
                                            session_data.append(row_data)

                            # Apply client-side filtering if selections were provided
                            filtered_data = session_data
                            if selections and session_data:
                                logger.info(f"Applying client-side filtering for selections: {selections}")
                                for sel_field, sel_values in selections.items():
                                    if not isinstance(sel_values, list):
                                        sel_values = [sel_values]

                                    # Find which dimension label corresponds to this field
                                    filter_label = None
                                    for i, field in enumerate(dim_defs):
                                        if field == sel_field or dim_labels_list[i] == sel_field:
                                            filter_label = dim_labels_list[i]
                                            break

                                    if filter_label:
                                        logger.info(f"Filtering by {filter_label} in {sel_values}")
                                        filtered_data = [row for row in filtered_data if str(row.get(filter_label, '')) in [str(v) for v in sel_values]]
                                        logger.info(f"Filtered from {len(session_data)} to {len(filtered_data)} rows")

                            # Apply client-side filtering for yearMonth (from filters dict)
                            if filters and 'yearMonth' in filters and session_data:
                                year_months = filters['yearMonth']
                                logger.info(f"Applying client-side yearMonth filtering: {year_months}")

                                # Find the date field label (likely "Дата")
                                date_label = None
                                for label in dim_labels_list:
                                    if 'дата' in label.lower() or 'date' in label.lower():
                                        date_label = label
                                        break

                                if date_label:
                                    logger.info(f"Using date field '{date_label}' for yearMonth filtering")

                                    def matches_year_month(date_str, target_year_months):
                                        """Check if date_str (DD.MM.YYYY) matches any target year-month (YYYY-MM)"""
                                        if not date_str:
                                            return False
                                        try:
                                            # Parse DD.MM.YYYY format
                                            parts = date_str.strip().split('.')
                                            if len(parts) == 3:
                                                day, month, year = parts
                                                # Create YYYY-MM format
                                                row_year_month = f"{year}-{month.zfill(2)}"
                                                return row_year_month in target_year_months
                                        except Exception as e:
                                            logger.warning(f"Failed to parse date '{date_str}': {e}")
                                            return False
                                        return False

                                    before_filter = len(filtered_data)
                                    filtered_data = [row for row in filtered_data if matches_year_month(row.get(date_label, ''), year_months)]
                                    logger.info(f"YearMonth filter: {before_filter} rows -> {len(filtered_data)} rows")
                                else:
                                    logger.warning("Could not find date field for yearMonth filtering")

                            # Clear selections
                            if selections:
                                try:
                                    self.engine_client.clear_all(app_handle)
                                except Exception:
                                    pass

                            # Destroy session object
                            try:
                                self.engine_client.send_request('DestroySessionObject', [session_handle], handle=app_handle)
                            except Exception:
                                pass

                            # Paginate the filtered data
                            total_filtered = len(filtered_data)
                            total_pages = (total_filtered + page_size - 1) // page_size if page_size > 0 else 1

                            # Apply pagination to filtered data
                            start_idx = (page - 1) * page_size
                            end_idx = start_idx + page_size
                            paginated_data = filtered_data[start_idx:end_idx]

                            if need_client_side_filtering:
                                logger.info(f"Client-side filtering applied: {len(session_data)} total rows -> {total_filtered} filtered rows")
                                logger.info(f"Returning page {page} with {len(paginated_data)} rows (indices {start_idx} to {end_idx - 1})")
                            else:
                                logger.info(f"No filtering applied: returning page {page} with {len(paginated_data)} rows")

                            # Return session data
                            result = {
                                'object_id': object_id,
                                'app_id': app_id,
                                'data': paginated_data,
                                'pagination': {
                                    'page': page,
                                    'page_size': page_size,
                                    'total_rows': total_filtered,
                                    'total_pages': total_pages,
                                    'has_next': page < total_pages,
                                    'has_previous': page > 1
                                }
                            }
                            logger.info(f"Returning session hypercube data with {len(paginated_data)} rows")
                            if paginated_data:
                                logger.info(f"First row being returned: {paginated_data[0]}")
                            return result
                        except Exception as session_err:
                            logger.error(f"Session hypercube failed: {session_err}")
                            # Destroy session object on error
                            try:
                                self.engine_client.send_request('DestroySessionObject', [session_handle], handle=app_handle)
                            except Exception:
                                pass
                            # Fall through to regular hypercube
                            use_regular_hypercube = True
                        # No fall through - return above

                if not use_regular_hypercube and not (is_collapsed_pivot or is_empty_pivot):
                    # Normal path: not a collapsed/empty pivot, use regular hypercube
                    use_regular_hypercube = True

                if use_regular_hypercube:
                    # Extract dimension fields and labels from layout (most reliable source)
                    dim_fields = []
                    dim_labels = []
                    for dim_info in hc_layout.get('qDimensionInfo', []):
                        # Use qFallbackTitle which contains the correct label
                        label = dim_info.get('qFallbackTitle', '')
                        # Get field name from qGroupFieldDefs
                        group_defs = dim_info.get('qGroupFieldDefs', [])
                        field = group_defs[0] if group_defs else label

                        dim_fields.append(field)
                        dim_labels.append(label)

                    # Extract measure labels from layout
                    measure_labels = []
                    for meas_info in hc_layout.get('qMeasureInfo', []):
                        label = meas_info.get('qFallbackTitle', '')
                        measure_labels.append(label)

                    # We still need to get properties for measure expressions (for field mapping)
                    # and to get the visual column order
                    properties = self.engine_client.send_request('GetFullPropertyTree', handle=obj_handle)
                    # The full property tree has the structure qPropEntry.qProperty.qHyperCubeDef
                    prop_entry = properties.get('qPropEntry', {})
                    prop_property = prop_entry.get('qProperty', {})
                    hc_def = prop_property.get('qHyperCubeDef', {})

                    measure_expressions = []
                    for measure_def in hc_def.get('qMeasures', []):
                        expression = measure_def.get('qDef', {}).get('qDef', '')
                        if expression:
                            measure_expressions.append(expression)

                    # Get the visual column order (how columns are displayed in the UI)
                    column_order = hc_def.get('qColumnOrder', [])
                    if not column_order:
                        # Fallback to natural order if no column order is specified
                        column_order = list(range(len(dim_fields) + len(measure_expressions)))

                    logger.info(f"Object has {len(dim_fields)} dimensions and {len(measure_expressions)} measures")
                    logger.info(f"Visual column order: {column_order}")

                    logger.info(f"Object hypercube has {total_rows} total rows after bookmark/selections")

                    # Fetch data directly from the object's hypercube using GetHyperCubeData
                    # Calculate how many rows to fetch
                    # Qlik has a limit on cells per request (~10,000 cells typically)
                    # With 15 columns (12 dims + 3 measures), max safe rows is ~500-600
                    num_columns = len(dim_fields) + len(measure_expressions)
                    max_safe_rows = min(500, 10000 // max(num_columns, 1))  # Conservative limit

                    fetch_rows = 1000 if filters else page_size
                    fetch_rows = min(fetch_rows, max_safe_rows)  # Cap at safe limit
                    start_row = 0 if filters else (page - 1) * page_size

                    # Ensure we don't fetch more than available
                    fetch_rows = min(fetch_rows, total_rows - start_row) if start_row < total_rows else 0

                    data_pages = []
                    if fetch_rows > 0:
                        logger.info(f"Fetching {fetch_rows} rows starting from row {start_row} ({num_columns} columns)")

                        try:
                            data_request = self.engine_client.send_request(
                                'GetHyperCubeData',
                                ['/qHyperCubeDef', [{'qTop': start_row, 'qLeft': 0, 'qWidth': num_columns, 'qHeight': fetch_rows}]],
                                handle=obj_handle
                            )
                            data_pages = data_request.get('qDataPages', [])
                        except Exception as e:
                            if 'too large' in str(e).lower():
                                logger.warning(f"Result too large error, reducing fetch size from {fetch_rows} to {fetch_rows // 2}")
                                # Retry with half the rows
                                fetch_rows = fetch_rows // 2
                                if fetch_rows > 0:
                                    data_request = self.engine_client.send_request(
                                        'GetHyperCubeData',
                                        ['/qHyperCubeDef', [{'qTop': start_row, 'qLeft': 0, 'qWidth': num_columns, 'qHeight': fetch_rows}]],
                                        handle=obj_handle
                                    )
                                    data_pages = data_request.get('qDataPages', [])
                                else:
                                    raise
                            else:
                                raise

                    all_rows = []
                    if data_pages:
                        matrix = data_pages[0].get('qMatrix', [])

                        # Create combined list of all labels (dims + measures) in hypercube order
                        all_labels = dim_labels + measure_labels

                        # IMPORTANT: qMatrix returns data cells in VISUAL column order, not hypercube order!
                        # We need to create a reverse mapping: hypercube_column -> cell_index
                        hypercube_to_cell = {}
                        for cell_index, hypercube_column in enumerate(column_order):
                            hypercube_to_cell[hypercube_column] = cell_index

                        for row in matrix:
                            row_data = {}

                            # Iterate through all labels in their hypercube order
                            # and map each to its corresponding cell based on column_order
                            for hypercube_column in range(len(all_labels)):
                                if hypercube_column not in hypercube_to_cell:
                                    continue

                                cell_index = hypercube_to_cell[hypercube_column]
                                if cell_index >= len(row):
                                    continue

                                label = all_labels[hypercube_column]
                                cell = row[cell_index]

                                # For dimensions (indices 0 to len(dim_labels)-1), use text
                                # For measures (indices >= len(dim_labels)), prefer numeric value
                                if hypercube_column < len(dim_labels):
                                    value = cell.get('qText', '')
                                else:
                                    value = cell.get('qNum', None)
                                    if value is None or str(value).lower() == 'nan':
                                        value = cell.get('qText', '')

                                row_data[label] = value

                            all_rows.append(row_data)

                    # Apply client-side filters if provided
                    filtered_rows = all_rows
                    if filters:
                        # Map field names to their display labels
                        field_to_label = dict(zip(dim_fields, dim_labels))

                        for field_name, field_value in filters.items():
                            # Get the label that corresponds to this field
                            filter_label = field_to_label.get(field_name, field_name)

                            logger.info(f"Filtering by {filter_label} (field: {field_name}) = {field_value}")
                            filtered_rows = [
                                row for row in filtered_rows
                                if str(row.get(filter_label, '')).strip() == str(field_value).strip()
                            ]

                    # Apply pagination to filtered results
                    # If filters were applied, total_rows is the filtered count; otherwise use the object's total
                    pagination_total = len(filtered_rows) if filters else total_rows

                    # Use actual fetched rows (might be less than requested page_size due to Qlik limits)
                    actual_page_size = len(filtered_rows) if not filters else page_size
                    total_pages = (pagination_total + actual_page_size - 1) // actual_page_size if pagination_total > 0 else 1

                    # For filtered data, we already have all rows in memory, so paginate from that
                    # For non-filtered data, we already fetched only the requested page
                    if filters:
                        offset = (page - 1) * page_size
                        data_rows = filtered_rows[offset:offset + page_size]
                    else:
                        data_rows = filtered_rows  # This is already the correct page

                    logger.info(f"Retrieved {len(data_rows)} rows from object '{object_id}' (page {page}/{total_pages}, total {pagination_total} rows)")

                    return {
                        'object_id': object_id,
                        'app_id': app_id,
                        'data': data_rows,
                        'pagination': {
                            'page': page,
                            'page_size': actual_page_size,  # Return actual size, not requested
                            'total_rows': pagination_total,
                            'total_pages': total_pages,
                            'has_next': page < total_pages,
                            'has_previous': page > 1
                        }
                    }

            finally:
                # Clear selections if they were applied
                if selections:
                    try:
                        logger.info("Clearing selections before disconnect")
                        self.engine_client.clear_all(app_handle)
                    except Exception as clear_error:
                        logger.warning(f"Failed to clear selections: {str(clear_error)}")

                # Always disconnect
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Error fetching data from object '{object_id}': {str(e)}")
            raise
