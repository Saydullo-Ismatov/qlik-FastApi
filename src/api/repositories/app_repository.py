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
                if not app_handle or app_handle == -1:
                    raise Exception(f"OpenDoc returned invalid handle {app_handle} for app '{app_id}'")

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
                if not app_handle or app_handle == -1:
                    raise Exception(f"OpenDoc returned invalid handle {app_handle} for app '{app_id}'")

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

        Routes to one of three fetch strategies depending on the object's hypercube
        layout: GetHyperCubePivotData (collapsed pivots), a session-hypercube
        fallback (when pivot data is incomplete), or the regular hypercube path.

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

            self.engine_client.connect()
            app_handle = None
            skip_outer_clear = False
            try:
                app_handle = self._open_app_with_context(app_id, bookmark_id, variables, selections)
                obj_handle = self._get_object_handle(app_handle, object_id)
                layout = self._get_layout_summary(obj_handle)

                is_collapsed_pivot = layout['visible_cols'] < layout['dim_count'] and layout['dim_count'] > 0
                is_empty_pivot = layout['total_rows'] == 0 and layout['dim_count'] > 0
                logger.info(
                    f"Pivot detection: visible_cols={layout['visible_cols']}, dim_count={layout['dim_count']}, "
                    f"is_collapsed_pivot={is_collapsed_pivot}, is_empty_pivot={is_empty_pivot}"
                )

                if is_collapsed_pivot or is_empty_pivot:
                    if is_collapsed_pivot:
                        logger.info(f"Pivot table has {layout['dim_count']} dimensions but only {layout['visible_cols']} visible columns - will try to get expanded data")
                    else:
                        logger.info("Hypercube has 0 rows but has dimensions - trying GetHyperCubePivotData for pivot table")

                    pivot_result = self._try_pivot_data_fetch(
                        app_handle, object_id, layout['hc_layout'], layout['dim_count'],
                        page, page_size, selections,
                    )
                    if pivot_result is not None:
                        pivot_result['app_id'] = app_id
                        return pivot_result

                    try:
                        session_result, server_side_filtered = self._fetch_via_session_hypercube(
                            app_handle, obj_handle, app_id, object_id,
                            layout['hc_layout'], page, page_size, filters, selections,
                        )
                        if server_side_filtered:
                            skip_outer_clear = True
                        return session_result
                    except Exception as session_err:
                        logger.error(f"Session hypercube failed: {session_err}")
                        # Fall through to regular hypercube path

                return self._fetch_via_regular_hypercube(
                    obj_handle, app_id, object_id,
                    layout['hc_layout'], layout['total_rows'],
                    page, page_size, filters,
                )

            finally:
                if selections and not skip_outer_clear and app_handle is not None:
                    try:
                        logger.info("Clearing selections before disconnect")
                        self.engine_client.clear_all(app_handle)
                    except Exception as clear_error:
                        logger.warning(f"Failed to clear selections: {str(clear_error)}")
                self.engine_client.disconnect()

        except Exception as e:
            logger.error(f"Error fetching data from object '{object_id}': {str(e)}")
            raise

    # ------------------------------------------------------------------
    # Helpers for get_object_data
    # ------------------------------------------------------------------

    def _open_app_with_context(self, app_id, bookmark_id, variables, selections):
        """Open the app and apply bookmark, variables, and initial selections."""
        result = self.engine_client.open_doc(app_id, no_data=False)
        app_handle = result['qReturn']['qHandle']
        if not app_handle or app_handle == -1:
            raise Exception(f"OpenDoc returned invalid handle {app_handle} for app '{app_id}' — app may not be accessible")

        if bookmark_id:
            self.engine_client.apply_bookmark(app_handle, bookmark_id)

        if variables:
            for var_name, var_value in variables.items():
                logger.info(f"Setting variable '{var_name}' to '{var_value}'")
                try:
                    self.engine_client.set_variable_value(app_handle, var_name, var_value)
                except Exception as var_error:
                    logger.warning(f"Failed to set variable '{var_name}': {str(var_error)}")

        if selections:
            self._apply_selections_to_engine(app_handle, selections)

        return app_handle

    def _apply_selections_to_engine(self, app_handle, selections):
        """Apply each selection on the Qlik engine; failures are logged but not raised."""
        for field_name, field_values in selections.items():
            if not isinstance(field_values, list):
                field_values = [field_values]
            logger.info(f"Applying selection on field '{field_name}' with values: {field_values}")
            try:
                self.engine_client.select_values(app_handle, field_name, field_values)
            except Exception as sel_error:
                logger.warning(f"Failed to apply selection on '{field_name}': {str(sel_error)}")

    def _get_object_handle(self, app_handle, object_id):
        obj_response = self.engine_client.send_request('GetObject', [object_id], handle=app_handle)
        return obj_response['qReturn']['qHandle']

    def _get_layout_summary(self, obj_handle):
        """Fetch GetLayout and return the size/structure summary used by the routing logic."""
        layout = self.engine_client.send_request('GetLayout', handle=obj_handle)
        hc_layout = layout.get('qLayout', {}).get('qHyperCube', {})
        qsize = hc_layout.get('qSize', {})
        total_rows = qsize.get('qcy', 0)
        visible_cols = qsize.get('qcx', 0)
        dim_count = len(hc_layout.get('qDimensionInfo', []))
        meas_count = len(hc_layout.get('qMeasureInfo', []))

        logger.info(f"Hypercube size from layout: {qsize}")
        logger.info(f"Total rows reported: {total_rows}")
        logger.info(f"Dimension count: {dim_count}")
        logger.info(f"Measure count: {meas_count}")

        return {
            'hc_layout': hc_layout,
            'total_rows': total_rows,
            'visible_cols': visible_cols,
            'dim_count': dim_count,
            'meas_count': meas_count,
        }

    def _try_pivot_data_fetch(self, app_handle, object_id, hc_layout, dim_count, page, page_size, selections):
        """
        Try GetHyperCubePivotData on the existing object.

        Returns the pivot data dict on success or empty result; returns None when
        the caller must fall back to the session-hypercube path.
        """
        # Re-apply selections at this point (mirrors original behavior — the engine
        # state may have been touched by the GetLayout above)
        if selections:
            self._apply_selections_to_engine(app_handle, selections)

        pivot_data = self.engine_client.get_pivot_data(
            app_handle=app_handle,
            object_id=object_id,
            page=page,
            page_size=page_size,
            selections={},
            bookmark_id=None,
        )

        if 'error' in pivot_data:
            logger.warning(f"Pivot data fetch failed: {pivot_data['error']} - falling back to regular hypercube")
            return None

        returned_data = pivot_data.get('data', [])
        if not returned_data:
            if selections:
                try:
                    self.engine_client.clear_all(app_handle)
                except Exception:
                    pass
            return pivot_data

        first_row_keys = set(returned_data[0].keys())
        dim_labels_in_data = sum(
            1 for dim_info in hc_layout.get('qDimensionInfo', [])
            if dim_info.get('qFallbackTitle', '') in first_row_keys
        )
        logger.info(f"Pivot data returned {dim_labels_in_data} dimensions out of {dim_count} expected")

        if dim_labels_in_data >= dim_count:
            if selections:
                try:
                    self.engine_client.clear_all(app_handle)
                except Exception:
                    pass
            return pivot_data

        logger.info(f"Pivot data is incomplete ({dim_labels_in_data}/{dim_count} dimensions) - falling back to regular hypercube for full expansion")
        return None

    def _fetch_via_session_hypercube(self, app_handle, obj_handle, app_id, object_id, hc_layout, page, page_size, filters, selections):
        """
        Build a flat session hypercube from the pivot object's dims/measures, fetch
        data, apply client-side filters, and paginate.

        Returns (result_dict, server_side_filtered) — server_side_filtered is True
        when the engine-level selections actually reduced the row count, in which
        case the caller should skip the outer clear_all (matches original behavior).
        """
        logger.info("Creating session object with flat table to get all dimensions")

        dim_defs, dim_labels_list = self._extract_session_dimensions(hc_layout)
        meas_defs, meas_labels_list = self._extract_session_measures(obj_handle, hc_layout)

        logger.info(f"Creating session hypercube with {len(dim_defs)} dimensions and {len(meas_defs)} measures")
        logger.info(f"Dimensions: {dim_labels_list}")
        logger.info(f"Measures: {meas_labels_list}")

        session_obj_def = {
            "qInfo": {"qType": "table"},
            "qHyperCubeDef": {
                "qDimensions": [{"qDef": {"qFieldDefs": [field]}} for field in dim_defs],
                "qMeasures": meas_defs,
                "qMode": "S",
                "qSuppressZero": False,
                "qSuppressMissing": False,
                "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": 0, "qWidth": 0}],
            },
        }

        session_obj = self.engine_client.send_request(
            'CreateSessionObject', [session_obj_def], handle=app_handle
        )
        session_handle = session_obj['qReturn']['qHandle']

        try:
            session_total_rows = self._get_session_total_rows(session_handle)
            logger.info(f"Session hypercube has {session_total_rows} rows")

            # Try server-side selections using the actual Qlik field names from the
            # session definition. The initial select_values may have failed if the
            # caller passed labels (e.g. 'Завод') instead of field names ('PRCTR').
            server_side_filtered = False
            if selections:
                new_total = self._apply_session_selections(
                    app_handle, session_handle, selections,
                    dim_defs, dim_labels_list, session_total_rows,
                )
                if new_total is not None:
                    server_side_filtered = True
                    session_total_rows = new_total
                    selections = {}  # selections handled at engine level; skip client-side filter

            force_full_fetch = filters.pop('_force_session_hypercube', False) if filters else False
            need_client_side_filtering = bool(filters) or bool(selections) or force_full_fetch

            total_cols = len(dim_defs) + len(meas_defs)
            session_data = self._fetch_session_rows(
                session_handle, session_total_rows, total_cols,
                dim_labels_list, meas_labels_list,
                page, page_size, fetch_all=need_client_side_filtering,
            )

            filtered_data = self._apply_session_client_filters(
                session_data, filters, selections, dim_defs, dim_labels_list,
            )

            if selections:
                try:
                    self.engine_client.clear_all(app_handle)
                except Exception:
                    pass

            try:
                self.engine_client.send_request('DestroySessionObject', [session_handle], handle=app_handle)
            except Exception:
                pass

            if need_client_side_filtering:
                total_filtered = len(filtered_data)
                total_pages = (total_filtered + page_size - 1) // page_size if page_size > 0 else 1
                start_idx = (page - 1) * page_size
                end_idx = start_idx + page_size
                paginated_data = filtered_data[start_idx:end_idx]
                logger.info(f"Client-side filtering applied: {len(session_data)} total rows -> {total_filtered} filtered rows")
                logger.info(f"Returning page {page} with {len(paginated_data)} rows (indices {start_idx} to {end_idx - 1})")
            else:
                total_filtered = session_total_rows
                total_pages = (total_filtered + page_size - 1) // page_size if page_size > 0 else 1
                paginated_data = filtered_data
                logger.info(f"No filtering applied: returning page {page} with {len(paginated_data)} rows")

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
                    'has_previous': page > 1,
                },
            }
            logger.info(f"Returning session hypercube data with {len(paginated_data)} rows")
            if paginated_data:
                logger.info(f"First row being returned: {paginated_data[0]}")
            return result, server_side_filtered

        except Exception:
            try:
                self.engine_client.send_request('DestroySessionObject', [session_handle], handle=app_handle)
            except Exception:
                pass
            raise

    def _extract_session_dimensions(self, hc_layout):
        dim_defs = []
        dim_labels_list = []
        for dim_info in hc_layout.get('qDimensionInfo', []):
            label = dim_info.get('qFallbackTitle', '')
            group_defs = dim_info.get('qGroupFieldDefs', [])
            field = group_defs[0] if group_defs else label
            if field:
                dim_defs.append(field)
                dim_labels_list.append(label)
        return dim_defs, dim_labels_list

    def _extract_session_measures(self, obj_handle, hc_layout):
        meas_defs = []
        meas_labels_list = []

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
            if library_id or expression:
                meas_defs.append(meas_def)
                meas_labels_list.append(label if label else f"Measure_{i}")
                logger.info(f"Measure {i} added: library_id={library_id}, has_expression={bool(expression)}")

        for i, meas_info in enumerate(hc_layout.get('qMeasureInfo', [])):
            label = meas_info.get('qFallbackTitle', '')
            if label:
                if i >= len(meas_labels_list):
                    meas_defs.append(f"[{label}]")
                    meas_labels_list.append(label)
                else:
                    meas_labels_list[i] = label

        return meas_defs, meas_labels_list

    def _get_session_total_rows(self, session_handle):
        session_layout = self.engine_client.send_request('GetLayout', handle=session_handle)
        session_hc = session_layout.get('qLayout', {}).get('qHyperCube', {})
        return session_hc.get('qSize', {}).get('qcy', 0)

    def _apply_session_selections(self, app_handle, session_handle, selections, dim_defs, dim_labels_list, original_total_rows):
        """
        Re-apply selections at the engine level using the actual Qlik field names
        from the session hypercube. Returns the new (reduced) total row count if
        selections actually narrowed the result, else None.
        """
        applied = False
        for sel_field, sel_values in selections.items():
            if not isinstance(sel_values, list):
                sel_values = [sel_values]
            actual_field = None
            for i, field in enumerate(dim_defs):
                if field == sel_field or dim_labels_list[i] == sel_field:
                    actual_field = field
                    break
            if actual_field:
                try:
                    logger.info(f"Applying Qlik selection on field '{actual_field}' (from '{sel_field}') with values: {sel_values}")
                    self.engine_client.select_values(app_handle, actual_field, sel_values)
                    applied = True
                except Exception as sel_err:
                    logger.warning(f"Failed to apply Qlik selection on '{actual_field}': {sel_err}")

        if not applied:
            return None

        new_total = self._get_session_total_rows(session_handle)
        logger.info(f"Session hypercube after selections: {new_total} rows (was {original_total_rows})")
        if new_total < original_total_rows:
            logger.info("Selections applied successfully in Qlik Engine - using server-side pagination")
            return new_total

        logger.info("Selections did not reduce row count - will use client-side filtering")
        return None

    def _fetch_session_rows(self, session_handle, total_rows, total_cols, dim_labels_list, meas_labels_list, page, page_size, fetch_all):
        """Fetch rows from a session hypercube (chunked when fetch_all, single page otherwise)."""
        rows = []
        if fetch_all:
            logger.info(f"Client-side filtering needed - fetching all {total_rows} rows in batches")
            MAX_CELLS = 10000
            chunk_size = min(2000, MAX_CELLS // max(total_cols, 1))
            logger.info(f"Calculated chunk size: {chunk_size} rows (columns: {total_cols}, max cells: {MAX_CELLS})")

            current_row = 0
            first_logged = False
            while current_row < total_rows:
                fetch_n = min(chunk_size, total_rows - current_row)
                logger.info(f"Fetching batch: rows {current_row} to {current_row + fetch_n - 1}")
                data_request = self.engine_client.send_request(
                    'GetHyperCubeData',
                    ['/qHyperCubeDef', [{'qTop': current_row, 'qLeft': 0, 'qWidth': total_cols, 'qHeight': fetch_n}]],
                    handle=session_handle,
                )
                data_pages = data_request.get('qDataPages', [])
                if data_pages:
                    matrix = data_pages[0].get('qMatrix', [])
                    logger.info(f"Batch returned {len(matrix)} rows with {len(matrix[0]) if matrix else 0} columns")
                    converted = self._matrix_to_session_rows(matrix, dim_labels_list, meas_labels_list)
                    if not first_logged and converted:
                        logger.info(f"First session row data: {converted[0]}")
                        first_logged = True
                    rows.extend(converted)
                current_row += fetch_n
            logger.info(f"Fetched total of {len(rows)} rows for client-side filtering")
        else:
            start_row = (page - 1) * page_size
            fetch_n = min(page_size, total_rows - start_row) if start_row < total_rows else 0
            if fetch_n > 0:
                logger.info(f"Fetching {fetch_n} rows from session hypercube starting at row {start_row}")
                data_request = self.engine_client.send_request(
                    'GetHyperCubeData',
                    ['/qHyperCubeDef', [{'qTop': start_row, 'qLeft': 0, 'qWidth': total_cols, 'qHeight': fetch_n}]],
                    handle=session_handle,
                )
                data_pages = data_request.get('qDataPages', [])
                if data_pages:
                    matrix = data_pages[0].get('qMatrix', [])
                    logger.info(f"Session qMatrix returned {len(matrix)} rows with {len(matrix[0]) if matrix else 0} columns")
                    if matrix:
                        logger.info(f"First row sample: {matrix[0][:3]}")
                    converted = self._matrix_to_session_rows(matrix, dim_labels_list, meas_labels_list)
                    if converted:
                        logger.info(f"First session row data: {converted[0]}")
                    rows.extend(converted)
        return rows

    def _matrix_to_session_rows(self, matrix, dim_labels_list, meas_labels_list):
        """Convert a session-hypercube qMatrix into a list of label-keyed dicts."""
        rows = []
        for row in matrix:
            row_data = {}
            for col_idx, cell in enumerate(row):
                if col_idx < len(dim_labels_list):
                    row_data[dim_labels_list[col_idx]] = cell.get('qText', '')
                elif col_idx < len(dim_labels_list) + len(meas_labels_list):
                    meas_idx = col_idx - len(dim_labels_list)
                    num_val = cell.get('qNum')
                    if num_val is not None and str(num_val).lower() not in ('nan', 'inf', '-inf'):
                        row_data[meas_labels_list[meas_idx]] = num_val
                    else:
                        row_data[meas_labels_list[meas_idx]] = cell.get('qText', '')
            rows.append(row_data)
        return rows

    def _apply_session_client_filters(self, session_data, filters, selections, dim_defs, dim_labels_list):
        """Apply client-side selection-equality filters and yearMonth filters to session rows."""
        filtered = session_data

        if selections and filtered:
            logger.info(f"Applying client-side filtering for selections: {selections}")
            for sel_field, sel_values in selections.items():
                if not isinstance(sel_values, list):
                    sel_values = [sel_values]
                filter_label = None
                for i, field in enumerate(dim_defs):
                    if field == sel_field or dim_labels_list[i] == sel_field:
                        filter_label = dim_labels_list[i]
                        break
                if filter_label:
                    logger.info(f"Filtering by {filter_label} in {sel_values}")
                    before = len(filtered)
                    str_values = [str(v) for v in sel_values]
                    filtered = [row for row in filtered if str(row.get(filter_label, '')) in str_values]
                    logger.info(f"Filtered from {before} to {len(filtered)} rows")

        if filters and 'yearMonth' in filters and filtered:
            year_months = filters['yearMonth']
            logger.info(f"Applying client-side yearMonth filtering: {year_months}")
            date_label = None
            for label in dim_labels_list:
                if 'дата' in label.lower() or 'date' in label.lower():
                    date_label = label
                    break

            if date_label:
                logger.info(f"Using date field '{date_label}' for yearMonth filtering")

                def matches_year_month(date_str, target_year_months):
                    if not date_str:
                        return False
                    try:
                        parts = date_str.strip().split('.')
                        if len(parts) == 3:
                            day, month, year = parts
                            return f"{year}-{month.zfill(2)}" in target_year_months
                    except Exception as e:
                        logger.warning(f"Failed to parse date '{date_str}': {e}")
                        return False
                    return False

                before = len(filtered)
                filtered = [row for row in filtered if matches_year_month(row.get(date_label, ''), year_months)]
                logger.info(f"YearMonth filter: {before} rows -> {len(filtered)} rows")
            else:
                logger.warning("Could not find date field for yearMonth filtering")

        return filtered

    def _fetch_via_regular_hypercube(self, obj_handle, app_id, object_id, hc_layout, total_rows, page, page_size, filters):
        """Fetch data directly from the object's hypercube (the flat-table path)."""
        dim_fields = []
        dim_labels = []
        for dim_info in hc_layout.get('qDimensionInfo', []):
            label = dim_info.get('qFallbackTitle', '')
            group_defs = dim_info.get('qGroupFieldDefs', [])
            field = group_defs[0] if group_defs else label
            dim_fields.append(field)
            dim_labels.append(label)

        measure_labels = [m.get('qFallbackTitle', '') for m in hc_layout.get('qMeasureInfo', [])]

        # qMatrix orders cells by visual column order, so we need qColumnOrder to map back
        properties = self.engine_client.send_request('GetFullPropertyTree', handle=obj_handle)
        prop_entry = properties.get('qPropEntry', {})
        prop_property = prop_entry.get('qProperty', {})
        hc_def = prop_property.get('qHyperCubeDef', {})

        column_order = hc_def.get('qColumnOrder', [])
        num_measures = len(measure_labels)
        if not column_order:
            column_order = list(range(len(dim_fields) + num_measures))

        logger.info(f"Object has {len(dim_fields)} dimensions and {num_measures} measures")
        logger.info(f"Visual column order: {column_order}")
        logger.info(f"Object hypercube has {total_rows} total rows after bookmark/selections")

        num_columns = len(dim_fields) + num_measures
        max_safe_rows = min(500, 10000 // max(num_columns, 1))
        fetch_rows = 1000 if filters else page_size
        fetch_rows = min(fetch_rows, max_safe_rows)
        start_row = 0 if filters else (page - 1) * page_size
        fetch_rows = min(fetch_rows, total_rows - start_row) if start_row < total_rows else 0

        data_pages = []
        if fetch_rows > 0:
            logger.info(f"Fetching {fetch_rows} rows starting from row {start_row} ({num_columns} columns)")
            try:
                data_request = self.engine_client.send_request(
                    'GetHyperCubeData',
                    ['/qHyperCubeDef', [{'qTop': start_row, 'qLeft': 0, 'qWidth': num_columns, 'qHeight': fetch_rows}]],
                    handle=obj_handle,
                )
                data_pages = data_request.get('qDataPages', [])
            except Exception as e:
                if 'too large' in str(e).lower():
                    logger.warning(f"Result too large error, reducing fetch size from {fetch_rows} to {fetch_rows // 2}")
                    fetch_rows = fetch_rows // 2
                    if fetch_rows > 0:
                        data_request = self.engine_client.send_request(
                            'GetHyperCubeData',
                            ['/qHyperCubeDef', [{'qTop': start_row, 'qLeft': 0, 'qWidth': num_columns, 'qHeight': fetch_rows}]],
                            handle=obj_handle,
                        )
                        data_pages = data_request.get('qDataPages', [])
                    else:
                        raise
                else:
                    raise

        all_rows = []
        if data_pages:
            matrix = data_pages[0].get('qMatrix', [])
            all_labels = dim_labels + measure_labels
            hypercube_to_cell = {hc_col: cell_idx for cell_idx, hc_col in enumerate(column_order)}

            for row in matrix:
                row_data = {}
                for hc_col in range(len(all_labels)):
                    if hc_col not in hypercube_to_cell:
                        continue
                    cell_index = hypercube_to_cell[hc_col]
                    if cell_index >= len(row):
                        continue
                    label = all_labels[hc_col]
                    cell = row[cell_index]
                    if hc_col < len(dim_labels):
                        value = cell.get('qText', '')
                    else:
                        value = cell.get('qNum', None)
                        if value is None or str(value).lower() == 'nan':
                            value = cell.get('qText', '')
                    row_data[label] = value
                all_rows.append(row_data)

        filtered_rows = all_rows
        if filters:
            field_to_label = dict(zip(dim_fields, dim_labels))
            for field_name, field_value in filters.items():
                filter_label = field_to_label.get(field_name, field_name)
                logger.info(f"Filtering by {filter_label} (field: {field_name}) = {field_value}")
                filtered_rows = [
                    row for row in filtered_rows
                    if str(row.get(filter_label, '')).strip() == str(field_value).strip()
                ]

        pagination_total = len(filtered_rows) if filters else total_rows
        total_pages = (pagination_total + page_size - 1) // page_size if pagination_total > 0 else 1

        if filters:
            offset = (page - 1) * page_size
            data_rows = filtered_rows[offset:offset + page_size]
        else:
            data_rows = filtered_rows

        logger.info(f"Retrieved {len(data_rows)} rows from object '{object_id}' (page {page}/{total_pages}, total {pagination_total} rows)")

        return {
            'object_id': object_id,
            'app_id': app_id,
            'data': data_rows,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_rows': pagination_total,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_previous': page > 1,
            },
        }
