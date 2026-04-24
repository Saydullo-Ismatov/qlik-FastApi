"""Qlik Sense Engine API client."""

import json
import websocket
import ssl
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging
import os
import re

from src.api.core.config import Settings
from src.api.clients.base import BaseClient

logger = logging.getLogger(__name__)


class QlikEngineClient(BaseClient):
    """Client for Qlik Sense Engine API using WebSocket."""

    def __init__(self, settings: Settings):
        """
        Initialize Qlik Engine API client.

        Args:
            settings: Application settings containing Qlik Sense configuration
        """
        super().__init__(settings)
        self.ws = None
        self.request_id = 0
        self.ws_timeout_seconds = settings.QLIK_WS_TIMEOUT
        self.ws_retries = 2  # Default retries

    def _get_next_request_id(self) -> int:
        """Get next request ID for JSON-RPC."""
        self.request_id += 1
        return self.request_id

    def connect(self, app_id: Optional[str] = None) -> None:
        """
        Connect to Engine API via WebSocket.

        Args:
            app_id: Optional application ID (not used in connection, kept for compatibility)

        Raises:
            ConnectionError: If connection fails after all retry attempts
        """
        # Try different WebSocket endpoints
        server_host = self.settings.QLIK_SENSE_HOST

        # Order and count of endpoints controlled by retries setting
        endpoints_all = [
            f"wss://{server_host}:{self.settings.QLIK_ENGINE_PORT}/app/engineData",
            f"wss://{server_host}:{self.settings.QLIK_ENGINE_PORT}/app",
            f"ws://{server_host}:{self.settings.QLIK_ENGINE_PORT}/app/engineData",
            f"ws://{server_host}:{self.settings.QLIK_ENGINE_PORT}/app",
        ]
        endpoints_to_try = endpoints_all[: max(1, min(self.ws_retries, len(endpoints_all)))]

        # Setup SSL context
        ssl_context = ssl.create_default_context()
        if not self.settings.QLIK_VERIFY_SSL:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        # Load certificates
        cert_path, key_path, ca_path = self.settings.get_cert_paths()
        if cert_path.exists() and key_path.exists():
            ssl_context.load_cert_chain(str(cert_path), str(key_path))

        if ca_path.exists():
            ssl_context.load_verify_locations(str(ca_path))

        # Headers for authentication
        headers = [
            f"X-Qlik-User: UserDirectory={self.settings.QLIK_USER_DIRECTORY}; UserId={self.settings.QLIK_USER_ID}"
        ]

        last_error = None
        for url in endpoints_to_try:
            try:
                if url.startswith("wss://"):
                    self.ws = websocket.create_connection(
                        url, sslopt={"context": ssl_context}, header=headers, timeout=self.ws_timeout_seconds
                    )
                else:
                    self.ws = websocket.create_connection(
                        url, header=headers, timeout=self.ws_timeout_seconds
                    )

                # Initial recv to establish session
                self.ws.recv()
                # Set a longer recv timeout for subsequent operations like GetLayout
                # which can take much longer than the connection timeout
                long_recv_timeout = self.ws_timeout_seconds * 5
                try:
                    self.ws.sock.settimeout(long_recv_timeout)
                    logger.info(f"Set recv timeout to {long_recv_timeout}s after connection (base: {self.ws_timeout_seconds}s)")
                except Exception as e:
                    logger.warning(f"Could not set recv timeout: {e}")
                return  # Success
            except Exception as e:
                last_error = e
                if self.ws:
                    try:
                        self.ws.close()
                    except Exception:
                        pass
                    self.ws = None
                continue

        raise ConnectionError(
            f"Failed to connect to Engine API. Last error: {str(last_error)}"
        )

    def disconnect(self) -> None:
        """Disconnect from Engine API."""
        if self.ws:
            self.ws.close()
            self.ws = None

    def send_request(
        self, method: str, params: List[Any] = None, handle: int = -1
    ) -> Dict[str, Any]:
        """
        Send JSON-RPC 2.0 request to Qlik Engine API and return response.

        Args:
            method: Engine API method name
            params: Method parameters list
            handle: Object handle for scoped operations (-1 for global)

        Returns:
            Response dictionary from Engine API

        Raises:
            ConnectionError: If not connected to Engine API
            Exception: If Engine API returns an error
        """
        if not self.ws:
            raise ConnectionError("Not connected to Engine API")

        request = {
            "jsonrpc": "2.0",
            "id": self._get_next_request_id(),
            "handle": handle,
            "method": method,
            "params": params or [],
        }

        self.ws.send(json.dumps(request))

        while True:
            data = self.ws.recv()
            if "result" in data or "error" in data:
                break

        response = json.loads(data)

        if "error" in response:
            raise Exception(f"Engine API error: {response['error']}")

        return response.get("result", {})

    def get_doc_list(self) -> List[Dict[str, Any]]:
        """
        Get list of available documents.

        Returns:
            List of document information dictionaries
        """
        try:
            result = self.send_request("GetDocList")
            doc_list = result.get("qDocList", [])

            if isinstance(doc_list, list):
                return doc_list
            else:
                return []

        except Exception as e:
            return []

    def open_doc(self, app_id: str, no_data: bool = True) -> Dict[str, Any]:
        """
        Open Qlik Sense application document.

        Args:
            app_id: Application ID to open
            no_data: If True, open without loading data (faster for metadata operations)

        Returns:
            Response with document handle

        Raises:
            Exception: If opening document fails
        """
        try:
            if no_data:
                return self.send_request("OpenDoc", [app_id, "", "", "", True])
            else:
                return self.send_request("OpenDoc", [app_id])
        except Exception as e:
            # If app is already open, try to get existing handle
            if "already open" in str(e).lower():
                try:
                    doc_list = self.get_doc_list()
                    for doc in doc_list:
                        if doc.get("qDocId") == app_id:
                            return {
                                "qReturn": {
                                    "qHandle": doc.get("qHandle", -1),
                                    "qGenericId": app_id
                                }
                            }
                except:
                    pass
            raise e

    def close_doc(self, app_handle: int) -> bool:
        """
        Close application document.

        Args:
            app_handle: Document handle to close

        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.send_request("CloseDoc", [], handle=app_handle)
            return result.get("qReturn", {}).get("qSuccess", False)
        except Exception:
            return False

    def get_active_doc(self) -> Dict[str, Any]:
        """
        Get currently active document if any.

        Returns:
            Active document information or empty dict
        """
        try:
            result = self.send_request("GetActiveDoc")
            return result
        except Exception:
            return {}

    def get_app_properties(self, app_handle: int) -> Dict[str, Any]:
        """
        Get app properties.

        Args:
            app_handle: Application handle

        Returns:
            App properties dictionary
        """
        return self.send_request("GetAppProperties", handle=app_handle)

    def get_script(self, app_handle: int) -> str:
        """
        Get load script.

        Args:
            app_handle: Application handle

        Returns:
            Load script as string
        """
        result = self.send_request("GetScript", [], handle=app_handle)
        return result.get("qScript", "")

    def set_script(self, app_handle: int, script: str) -> bool:
        """
        Set load script.

        Args:
            app_handle: Application handle
            script: Load script content

        Returns:
            True if successful, False otherwise
        """
        result = self.send_request("SetScript", [script], handle=app_handle)
        return result.get("qReturn", {}).get("qSuccess", False)

    def do_save(self, app_handle: int, file_name: Optional[str] = None) -> bool:
        """
        Save app.

        Args:
            app_handle: Application handle
            file_name: Optional file name

        Returns:
            True if successful, False otherwise
        """
        params = {}
        if file_name:
            params["qFileName"] = file_name
        result = self.send_request("DoSave", params, handle=app_handle)
        return result.get("qReturn", {}).get("qSuccess", False)

    def get_objects(
        self, app_handle: int, object_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get app objects.

        Args:
            app_handle: Application handle
            object_type: Optional filter by object type

        Returns:
            List of object information dictionaries
        """
        if object_type:
            params = {
                "qOptions": {
                    "qTypes": [object_type],
                    "qIncludeSessionObjects": True,
                    "qData": {},
                }
            }
        else:
            params = {
                "qOptions": {
                    "qIncludeSessionObjects": True,
                    "qData": {},
                }
            }

        logger.debug(f"get_objects params: {params}")

        result = self.send_request("GetObjects", params, handle=app_handle)

        if "error" in str(result) or "Missing Types" in str(result):
            logger.debug(f"get_objects error result: {result}")

        return result.get("qList", {}).get("qItems", [])

    def get_sheets(self, app_handle: int) -> List[Dict[str, Any]]:
        """
        Get app sheets.

        Args:
            app_handle: Application handle

        Returns:
            List of sheet information dictionaries
        """
        try:
            sheet_list_def = {
                "qInfo": {"qType": "SheetList"},
                "qAppObjectListDef": {
                    "qType": "sheet",
                    "qData": {
                        "title": "/qMetaDef/title",
                        "description": "/qMetaDef/description",
                        "thumbnail": "/thumbnail",
                        "cells": "/cells",
                        "rank": "/rank",
                        "columns": "/columns",
                        "rows": "/rows"
                    }
                }
            }

            create_result = self.send_request("CreateSessionObject", [sheet_list_def], handle=app_handle)

            if "qReturn" not in create_result or "qHandle" not in create_result["qReturn"]:
                logger.warning(f"Failed to create SheetList object: {create_result}")
                return []

            sheet_list_handle = create_result["qReturn"]["qHandle"]
            layout_result = self.send_request("GetLayout", [], handle=sheet_list_handle)
            if "qLayout" not in layout_result or "qAppObjectList" not in layout_result["qLayout"]:
                logger.warning(f"No sheet list in layout: {layout_result}")
                return []

            sheets = layout_result["qLayout"]["qAppObjectList"]["qItems"]
            logger.info(f"Found {len(sheets)} sheets")
            return sheets

        except Exception as e:
            logger.error(f"get_sheets exception: {str(e)}")
            return []

    def get_fields(self, app_handle: int) -> Dict[str, Any]:
        """
        Get app fields using GetTablesAndKeys method.

        Args:
            app_handle: Application handle

        Returns:
            Dictionary with fields information, tables count, and total fields
        """
        try:
            result = self.send_request(
                "GetTablesAndKeys",
                [
                    {"qcx": 1000, "qcy": 1000},  # Max dimensions
                    {"qcx": 0, "qcy": 0},  # Min dimensions
                    30,  # Max tables
                    True,  # Include system tables
                    False,  # Include hidden fields
                ],
                handle=app_handle,
            )

            fields_info = []

            if "qtr" in result:
                for table in result["qtr"]:
                    table_name = table.get("qName", "Unknown")

                    if "qFields" in table:
                        for field in table["qFields"]:
                            field_info = {
                                "field_name": field.get("qName", ""),
                                "table_name": table_name,
                                "data_type": field.get("qType", ""),
                                "is_key": field.get("qIsKey", False),
                                "is_system": field.get("qIsSystem", False),
                                "is_hidden": field.get("qIsHidden", False),
                                "is_semantic": field.get("qIsSemantic", False),
                                "distinct_values": field.get("qnTotalDistinctValues", 0),
                                "present_distinct_values": field.get("qnPresentDistinctValues", 0),
                                "rows_count": field.get("qnRows", 0),
                                "subset_ratio": field.get("qSubsetRatio", 0),
                                "key_type": field.get("qKeyType", ""),
                                "tags": field.get("qTags", []),
                            }
                            fields_info.append(field_info)

            return {
                "fields": fields_info,
                "tables_count": len(result.get("qtr", [])),
                "total_fields": len(fields_info),
            }

        except Exception as e:
            return {"error": str(e), "details": "Error in get_fields method"}

    def get_tables(self, app_handle: int) -> List[Dict[str, Any]]:
        """
        Get app tables.

        Args:
            app_handle: Application handle

        Returns:
            List of table information dictionaries
        """
        result = self.send_request("GetTablesList", handle=app_handle)
        return result.get("qtr", [])

    def create_session_object(
        self, app_handle: int, obj_def: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create session object.

        Args:
            app_handle: Application handle
            obj_def: Object definition dictionary

        Returns:
            Response with object handle
        """
        return self.send_request(
            "CreateSessionObject", {"qProp": obj_def}, handle=app_handle
        )

    def get_object(self, app_handle: int, object_id: str) -> Dict[str, Any]:
        """
        Get object by ID.

        Args:
            app_handle: Application handle
            object_id: Object ID

        Returns:
            Object information dictionary
        """
        return self.send_request("GetObject", {"qId": object_id}, handle=app_handle)

    def evaluate_expression(self, app_handle: int, expression: str) -> Any:
        """
        Evaluate expression.

        Args:
            app_handle: Application handle
            expression: Qlik expression to evaluate

        Returns:
            Evaluation result
        """
        result = self.send_request(
            "Evaluate", {"qExpression": expression}, handle=app_handle
        )
        return result.get("qReturn", {})

    def select_in_field(
        self, app_handle: int, field_name: str, values: List[str], toggle: bool = False
    ) -> bool:
        """
        Select values in field.

        Args:
            app_handle: Application handle
            field_name: Field name
            values: List of values to select
            toggle: Whether to toggle selection

        Returns:
            True if successful, False otherwise
        """
        params = {"qFieldName": field_name, "qValues": values, "qToggleMode": toggle}
        result = self.send_request("SelectInField", params, handle=app_handle)
        return result.get("qReturn", False)

    def clear_selections(self, app_handle: int, locked_also: bool = False) -> bool:
        """
        Clear all selections.

        Args:
            app_handle: Application handle
            locked_also: Whether to clear locked selections too

        Returns:
            True if successful, False otherwise
        """
        params = {"qLockedAlso": locked_also}
        result = self.send_request("ClearAll", params, handle=app_handle)
        return result.get("qReturn", False)

    def get_current_selections(self, app_handle: int) -> List[Dict[str, Any]]:
        """
        Get current selections.

        Args:
            app_handle: Application handle

        Returns:
            List of current selections
        """
        result = self.send_request("GetCurrentSelections", handle=app_handle)
        return result.get("qSelections", [])

    def create_hypercube(
        self,
        app_handle: int,
        dimensions: List[str],
        measures: List[str],
        max_rows: int = 1000,
    ) -> Dict[str, Any]:
        """
        Create hypercube for data extraction with proper structure.

        Args:
            app_handle: Application handle
            dimensions: List of dimension field expressions
            measures: List of measure expressions
            max_rows: Maximum number of rows to fetch

        Returns:
            Dictionary containing hypercube data and metadata
        """
        try:
            hypercube_def = {
                "qDimensions": [
                    {
                        "qDef": {
                            "qFieldDefs": [dim],
                            "qSortCriterias": [
                                {
                                    "qSortByState": 0,
                                    "qSortByFrequency": 0,
                                    "qSortByNumeric": 1,
                                    "qSortByAscii": 1,
                                    "qSortByLoadOrder": 0,
                                    "qSortByExpression": 0,
                                    "qExpression": {"qv": ""},
                                }
                            ],
                        },
                        "qNullSuppression": False,
                        "qIncludeElemValue": True,
                    }
                    for dim in dimensions
                ],
                "qMeasures": [
                    {
                        "qDef": {"qDef": measure, "qLabel": f"Measure_{i}"},
                        "qSortBy": {"qSortByNumeric": -1, "qSortByLoadOrder": 0},
                    }
                    for i, measure in enumerate(measures)
                ],
                "qInitialDataFetch": [
                    {
                        "qTop": 0,
                        "qLeft": 0,
                        "qHeight": max_rows,
                        "qWidth": len(dimensions) + len(measures),
                    }
                ],
                "qSuppressZero": False,
                "qSuppressMissing": False,
                "qMode": "S",
                "qInterColumnSortOrder": list(range(len(dimensions) + len(measures))),
            }

            obj_def = {
                "qInfo": {
                    "qId": f"hypercube-{len(dimensions)}d-{len(measures)}m",
                    "qType": "HyperCube",
                },
                "qHyperCubeDef": hypercube_def,
            }

            result = self.send_request(
                "CreateSessionObject", [obj_def], handle=app_handle
            )

            if "qReturn" not in result or "qHandle" not in result["qReturn"]:
                return {"error": "Failed to create hypercube", "response": result}

            cube_handle = result["qReturn"]["qHandle"]

            # For complex hypercubes (many dimensions), extend the recv timeout
            # because GetLayout can take much longer to calculate
            original_timeout = self.ws_timeout_seconds
            extended_timeout = max(original_timeout, len(dimensions) * 20)  # 20s per dimension
            if extended_timeout > original_timeout:
                logger.info(f"Extending WebSocket recv timeout to {extended_timeout}s for {len(dimensions)}-dimension hypercube")
                try:
                    self.ws.sock.settimeout(extended_timeout)
                except Exception:
                    pass

            layout = self.send_request("GetLayout", [], handle=cube_handle)

            # Restore original timeout
            if extended_timeout > original_timeout:
                try:
                    self.ws.sock.settimeout(original_timeout)
                except Exception:
                    pass

            if "qLayout" not in layout or "qHyperCube" not in layout["qLayout"]:
                return {"error": "No hypercube in layout", "layout": layout}

            hypercube = layout["qLayout"]["qHyperCube"]

            return {
                "hypercube_handle": cube_handle,
                "hypercube_data": hypercube,
                "dimensions": dimensions,
                "measures": measures,
                "total_rows": hypercube.get("qSize", {}).get("qcy", 0),
                "total_columns": hypercube.get("qSize", {}).get("qcx", 0),
            }

        except Exception as e:
            return {"error": str(e), "details": "Error in create_hypercube method"}

    def get_hypercube_data(
        self,
        hypercube_handle: int,
        page_top: int = 0,
        page_height: int = 1000,
        page_left: int = 0,
        page_width: int = 50,
    ) -> Dict[str, Any]:
        """
        Get data from existing hypercube with pagination.

        Args:
            hypercube_handle: Hypercube object handle
            page_top: Starting row
            page_height: Number of rows
            page_left: Starting column
            page_width: Number of columns

        Returns:
            Hypercube data dictionary
        """
        try:
            params = [
                {
                    "qPath": "/qHyperCubeDef",
                    "qPages": [
                        {
                            "qTop": page_top,
                            "qLeft": page_left,
                            "qHeight": page_height,
                            "qWidth": page_width,
                        }
                    ],
                }
            ]

            result = self.send_request(
                "GetHyperCubeData", params, handle=hypercube_handle
            )
            return result

        except Exception as e:
            return {"error": str(e), "details": "Error in get_hypercube_data method"}

    def get_field_values(
        self,
        app_handle: int,
        field_name: str,
        max_values: int = 100,
        include_frequency: bool = True,
    ) -> Dict[str, Any]:
        """
        Get field values with frequency information using ListObject.

        Args:
            app_handle: Application handle
            field_name: Field name
            max_values: Maximum number of values to return
            include_frequency: Whether to include frequency information

        Returns:
            Dictionary with field values and metadata
        """
        try:
            list_def = {
                "qInfo": {"qId": f"field-values-{field_name}", "qType": "ListObject"},
                "qListObjectDef": {
                    "qStateName": "$",
                    "qLibraryId": "",
                    "qDef": {
                        "qFieldDefs": [field_name],
                        "qFieldLabels": [],
                        "qSortCriterias": [
                            {
                                "qSortByState": 0,
                                "qSortByFrequency": 1 if include_frequency else 0,
                                "qSortByNumeric": 1,
                                "qSortByAscii": 1,
                                "qSortByLoadOrder": 0,
                                "qSortByExpression": 0,
                                "qExpression": {"qv": ""},
                            }
                        ],
                    },
                    "qInitialDataFetch": [
                        {"qTop": 0, "qLeft": 0, "qHeight": max_values, "qWidth": 1}
                    ],
                },
            }

            result = self.send_request(
                "CreateSessionObject", [list_def], handle=app_handle
            )

            if "qReturn" not in result or "qHandle" not in result["qReturn"]:
                return {"error": "Failed to create session object", "response": result}

            list_handle = result["qReturn"]["qHandle"]

            layout = self.send_request("GetLayout", [], handle=list_handle)

            if "qLayout" not in layout or "qListObject" not in layout["qLayout"]:
                try:
                    self.send_request(
                        "DestroySessionObject",
                        [f"field-values-{field_name}"],
                        handle=app_handle,
                    )
                except:
                    pass
                return {"error": "No list object in layout", "layout": layout}

            list_object = layout["qLayout"]["qListObject"]
            values_data = []

            for page in list_object.get("qDataPages", []):
                for row in page.get("qMatrix", []):
                    if row and len(row) > 0:
                        cell = row[0]
                        value_info = {
                            "value": cell.get("qText", ""),
                            "state": cell.get("qState", "O"),
                            "numeric_value": cell.get("qNum", None),
                            "is_numeric": cell.get("qIsNumeric", False),
                        }

                        if "qFrequency" in cell:
                            value_info["frequency"] = cell.get("qFrequency", 0)

                        values_data.append(value_info)

            field_info = {
                "field_name": field_name,
                "values": values_data,
                "total_values": list_object.get("qSize", {}).get("qcy", 0),
                "returned_count": len(values_data),
                "dimension_info": list_object.get("qDimensionInfo", {}),
            }

            try:
                self.send_request(
                    "DestroySessionObject",
                    [f"field-values-{field_name}"],
                    handle=app_handle,
                )
            except Exception as cleanup_error:
                field_info["cleanup_warning"] = str(cleanup_error)

            return field_info

        except Exception as e:
            return {"error": str(e), "details": "Error in get_field_values method"}

    def get_measures(self, app_handle: int) -> List[Dict[str, Any]]:
        """
        Get master measures using GetAllInfos + GetMeasure + GetLayout.

        Args:
            app_handle: Application handle

        Returns:
            List of measure information dictionaries
        """
        try:
            all_infos_result = self.send_request("GetAllInfos", [], handle=app_handle)
            if "qInfos" not in all_infos_result:
                return []

            measures = []
            for info in all_infos_result["qInfos"]:
                if info.get("qType") == "measure":
                    measure_id = info.get("qId")
                    if measure_id:
                        try:
                            measure_result = self.send_request("GetMeasure", [measure_id], handle=app_handle)
                            if "qReturn" in measure_result:
                                measure_handle = measure_result["qReturn"]["qHandle"]
                                if measure_handle:
                                    layout_result = self.send_request("GetLayout", [], handle=measure_handle)
                                    if "qLayout" in layout_result:
                                        layout = layout_result["qLayout"]
                                        measure_data = {
                                            "qInfo": layout.get("qInfo", {}),
                                            "qMeta": layout.get("qMeta", {}),
                                            "qMeasure": layout.get("qMeasure", {}),
                                            "qData": {}
                                        }
                                        measures.append(measure_data)
                                    else:
                                        measures.append({
                                            "qInfo": info,
                                            "qMeta": {"title": f"Measure {measure_id}"},
                                            "qMeasure": {},
                                            "qData": {}
                                        })
                                else:
                                    measures.append({
                                        "qInfo": info,
                                        "qMeta": {"title": f"Measure {measure_id}"},
                                        "qMeasure": {},
                                        "qData": {}
                                    })
                            else:
                                measures.append({
                                    "qInfo": info,
                                    "qMeta": {"title": f"Measure {measure_id}"},
                                    "qMeasure": {},
                                    "qData": {}
                                })
                        except Exception as e:
                            logger.warning(f"Could not get details for measure {measure_id}: {e}")
                            measures.append({
                                "qInfo": info,
                                "qMeta": {"title": f"Measure {measure_id}"},
                                "qMeasure": {},
                                "qData": {}
                            })

            return measures
        except Exception as e:
            logger.error(f"Error getting measures with GetAllInfos: {e}")
            try:
                result = self.send_request("GetMeasureList", handle=app_handle)
                return result.get("qMeasureList", {}).get("qItems", [])
            except Exception as e2:
                logger.error(f"Error getting measures with GetMeasureList: {e2}")
                return []

    def get_dimensions(self, app_handle: int) -> List[Dict[str, Any]]:
        """
        Get master dimensions.

        Args:
            app_handle: Application handle

        Returns:
            List of dimension information dictionaries
        """
        result = self.send_request("GetDimensionList", handle=app_handle)
        return result.get("qDimensionList", {}).get("qItems", [])

    def get_variables(self, app_handle: int) -> List[Dict[str, Any]]:
        """
        Get variables.

        Args:
            app_handle: Application handle

        Returns:
            List of variable information dictionaries
        """
        result = self.send_request("GetVariableList", handle=app_handle)
        return result.get("qVariableList", {}).get("qItems", [])

    def set_variable_value(
        self, app_handle: int, var_name: str, value: str
    ) -> bool:
        """
        Set a variable value by name.

        Args:
            app_handle: Application handle
            var_name: Variable name (e.g., "vChooseType")
            value: Value to set (e.g., "1", "2", "3")

        Returns:
            True if successful, False otherwise

        Raises:
            Exception: If setting variable fails
        """
        try:
            logger.info(f"Setting variable '{var_name}' to '{value}'")

            # Get the variable object
            var_result = self.send_request(
                "GetVariableByName", [var_name], handle=app_handle
            )
            var_handle = var_result.get("qReturn", {}).get("qHandle")

            if not var_handle:
                raise Exception(f"Could not get handle for variable '{var_name}'")

            # Set the string value
            result = self.send_request(
                "SetStringValue", [value], handle=var_handle
            )

            success = result.get("qReturn", False)
            logger.info(f"Set variable '{var_name}' to '{value}': success={success}")
            return success

        except Exception as e:
            logger.error(f"Error setting variable '{var_name}': {str(e)}")
            raise

    def _extract_fields_from_expression(self, expression: str) -> List[str]:
        """
        Extract field names from a complex expression.

        Args:
            expression: Qlik expression

        Returns:
            List of field names found in expression
        """
        fields = []
        if not expression:
            return fields
        bracket_fields = re.findall(r'\[([^\]]+)\]', expression)
        fields.extend(bracket_fields)
        return list(set(fields))

    def get_field(self, app_handle: int, field_name: str) -> Dict[str, Any]:
        """
        Get a field object for making selections.

        Args:
            app_handle: Application handle
            field_name: Name of the field

        Returns:
            Dictionary with field handle and info

        Raises:
            Exception: If field cannot be retrieved
        """
        try:
            result = self.send_request("GetField", [field_name], handle=app_handle)
            return result
        except Exception as e:
            logger.error(f"Error getting field '{field_name}': {str(e)}")
            raise

    def select_values(
        self,
        app_handle: int,
        field_name: str,
        values: List[str],
        toggle: bool = False
    ) -> bool:
        """
        Select values in a field.

        Args:
            app_handle: Application handle
            field_name: Name of the field to select in
            values: List of values to select
            toggle: If True, toggle selection; if False, replace selection

        Returns:
            True if selection was successful

        Raises:
            Exception: If selection fails
        """
        try:
            logger.info(f"Selecting values in field '{field_name}': {values}")

            # Get field object
            field_result = self.get_field(app_handle, field_name)
            field_handle = field_result.get("qReturn", {}).get("qHandle")

            if not field_handle:
                raise Exception(f"Could not get handle for field '{field_name}'")

            # Select values.
            # softLock=True: allow this selection to override soft-locked fields
            # (fields locked by a bookmark).  Without this, a SelectValues call
            # on a field that the bookmark locked silently returns qReturn=False
            # and the filter has no effect.
            result = self.send_request(
                "SelectValues",
                [
                    [{"qText": str(value)} for value in values],
                    toggle,
                    True  # softLock = True — override bookmark locks
                ],
                handle=field_handle
            )

            success = result.get("qReturn", False)
            logger.info(f"Selection in '{field_name}' successful: {success}")
            return success

        except Exception as e:
            logger.error(f"Error selecting values in field '{field_name}': {str(e)}")
            raise

    def get_object_type(self, obj_handle: int) -> str:
        """
        Get the type of a Qlik object (e.g., 'pivot-table', 'table', 'barchart').

        Args:
            obj_handle: Object handle

        Returns:
            Object type string, or 'unknown' if not determinable
        """
        try:
            info = self.send_request("GetInfo", [], handle=obj_handle)
            return info.get("qInfo", {}).get("qType", "unknown")
        except Exception:
            return "unknown"

    def apply_bookmark(self, app_handle: int, bookmark_id: str) -> bool:
        """
        Apply a bookmark to the app session.

        Args:
            app_handle: Application handle
            bookmark_id: ID of the bookmark to apply

        Returns:
            True if bookmark was applied successfully
        """
        try:
            result = self.send_request("ApplyBookmark", [bookmark_id], handle=app_handle)
            success = result.get("qSuccess", False)
            logger.info(f"Applied bookmark '{bookmark_id}': success={success}")
            return success
        except Exception as e:
            logger.error(f"Error applying bookmark '{bookmark_id}': {e}")
            return False

    def _flatten_pivot_node(
        self,
        node: Dict,
        path: List[str],
        dim_labels: List[str],
        q_data: List,
        data_idx_ref: List[int],
        flat_rows: List[Dict],
        meas_labels: List[str],
    ) -> None:
        """
        Recursively traverse a pivot tree node and emit flat rows at the leaves.

        Qlik's GetHyperCubePivotData returns qLeft as a tree where each node
        has qSubNodes for child dimensions. This method flattens the tree into
        individual rows, matching leaves to their corresponding qData measure rows.

        Args:
            node: Current pivot tree node (NxPivotCell with optional qSubNodes)
            path: List of dimension values accumulated so far from root to current node
            dim_labels: List of dimension label names
            q_data: List of measure value rows from qData
            data_idx_ref: Mutable single-element list holding the current qData index
            flat_rows: Output list that accumulates completed rows
            meas_labels: List of measure label names
        """
        current_val = node.get("qText", "")
        new_path = path + [current_val]
        sub_nodes = node.get("qSubNodes", [])

        if not sub_nodes:
            # Leaf node - build one flat row with all dimension + measure values
            row = {}
            for i, val in enumerate(new_path):
                label = dim_labels[i] if i < len(dim_labels) else f"dim{i}"
                row[label] = val

            # Add measures from the corresponding qData row
            if data_idx_ref[0] < len(q_data):
                data_cells = q_data[data_idx_ref[0]]
                if isinstance(data_cells, list):
                    for col_idx, cell in enumerate(data_cells):
                        label = meas_labels[col_idx] if col_idx < len(meas_labels) else f"measure{col_idx}"
                        if isinstance(cell, dict):
                            num_val = cell.get("qNum")
                            text_val = cell.get("qText", "")
                            if num_val is not None and str(num_val).lower() not in ("nan", "inf", "-inf"):
                                row[label] = num_val
                            else:
                                row[label] = text_val
                        else:
                            row[label] = cell
                data_idx_ref[0] += 1

            flat_rows.append(row)
        else:
            # Intermediate node - recurse into children
            for child in sub_nodes:
                self._flatten_pivot_node(
                    child, new_path, dim_labels, q_data, data_idx_ref, flat_rows, meas_labels
                )

    def get_pivot_data(
        self,
        app_handle: int,
        object_id: str,
        page: int = 1,
        page_size: int = 100,
        selections: Dict = None,
        bookmark_id: str = None,
    ) -> Dict[str, Any]:
        """
        Get data from a pivot-table Qlik object using GetHyperCubePivotData.

        This is significantly faster than creating a session hypercube because
        it reads from the already-computed pivot object.

        When a bookmark_id is provided, it is applied before fetching data.
        This allows the pivot table to show its fully-expanded filtered state
        (e.g. a date-filtered bookmark reveals all 12 dimension values).

        The qLeft response from Qlik is a nested tree (via qSubNodes) when the
        pivot is in expanded state. This method recursively flattens that tree
        to produce flat rows with all dimension values.

        NOTE: The ``selections`` dict is applied as **client-side filters** on
        the Python side after all rows are fetched.  We deliberately do NOT call
        Qlik's SelectValues because doing so after ApplyBookmark forces a full
        pivot recompute on the Qlik server, which is extremely slow and memory
        intensive.  Since the bookmark already caches the pivot, fetching all
        rows and filtering in Python is both faster and much cheaper.

        Args:
            app_handle: Application handle
            object_id: ID of the pivot table object
            page: Page number (1-based)
            page_size: Number of rows per page
            selections: Optional dict of field -> [values] for client-side filtering
            bookmark_id: Optional bookmark ID to apply before fetching data

        Returns:
            Dictionary with data rows and pagination info
        """
        try:
            # Apply bookmark first — this gives us the cached, expanded pivot state.
            if bookmark_id:
                self.apply_bookmark(app_handle, bookmark_id)

            # NOTE: We do NOT call select_values here even if `selections` is provided.
            # SelectValues after ApplyBookmark triggers a full server-side recompute.
            # We apply the filters client-side after fetching all rows (see below).

            # Get the existing object
            obj_resp = self.send_request("GetObject", [object_id], handle=app_handle)
            obj_handle = obj_resp["qReturn"]["qHandle"]

            # Get properties for dimension/measure labels
            props = self.send_request("GetProperties", [], handle=obj_handle)
            hc_def = props.get("qProp", {}).get("qHyperCubeDef", {})

            dim_fields = []  # raw field names (used to match selection keys)
            dim_labels = []  # display labels (used as row keys in output)
            for d in hc_def.get("qDimensions", []):
                field_defs = d.get("qDef", {}).get("qFieldDefs", [])
                field_labels = d.get("qDef", {}).get("qFieldLabels", [])
                field = field_defs[0] if field_defs else None
                label = field_labels[0] if field_labels else None
                dim_fields.append(field if field else "")
                dim_labels.append(label if label else (field if field else ""))

            meas_labels = []
            for i, m in enumerate(hc_def.get("qMeasures", [])):
                label = m.get("qDef", {}).get("qLabel", "")
                expr = m.get("qDef", {}).get("qDef", "")
                meas_labels.append(label if label else (expr if expr else f"Measure_{i}"))

            n_meas = len(meas_labels)
            n_dims = len(dim_labels)

            # Get qNoOfLeftDims to know how many dimensions are on the left
            q_no_of_left_dims = hc_def.get("qNoOfLeftDims", n_dims)

            # GetLayout to know total row count (very fast — reads cached state)
            layout = self.send_request("GetLayout", [], handle=obj_handle)
            hc = layout.get("qLayout", {}).get("qHyperCube", {})
            total_rows = hc.get("qSize", {}).get("qcy", 0)

            logger.info(f"Pivot object '{object_id}' qSize reports {total_rows} rows")
            logger.info(f"Pivot has {n_dims} dimensions and {n_meas} measures")
            logger.info(f"Pivot qNoOfLeftDims: {q_no_of_left_dims}")

            # For pivot tables, qSize can be 0 even when data exists
            # We need to actually fetch the data to know if there are rows
            # So we'll proceed with the fetch regardless of qSize

            # Determine how many rows to fetch from Qlik.
            # If client-side filters are requested we must fetch ALL rows so we
            # can filter them before paginating.  Otherwise just fetch the page.
            need_all_rows = bool(selections)
            fetch_height = total_rows if need_all_rows else page_size
            fetch_offset = 0 if need_all_rows else (page - 1) * page_size

            logger.info(
                f"Fetching {'all' if need_all_rows else 'page'} rows: "
                f"top={fetch_offset}, height={fetch_height}"
            )

            logger.info(f"Requesting pivot data: qTop={fetch_offset}, qHeight={fetch_height}, qWidth={max(n_meas, 1)}, qLeft={q_no_of_left_dims}")

            data_resp = self.send_request(
                "GetHyperCubePivotData",
                [
                    "/qHyperCubeDef",
                    [{"qLeft": 0, "qTop": fetch_offset, "qWidth": max(n_meas, 1), "qHeight": fetch_height}]
                ],
                handle=obj_handle
            )

            pages_data = data_resp.get("qDataPages", [])
            logger.info(f"Received {len(pages_data)} data pages from pivot")
            flat_rows = []

            if pages_data and len(pages_data) > 0:
                q_left = pages_data[0].get("qLeft", [])
                q_data = pages_data[0].get("qData", [])

                # Log the structure of qLeft to understand the format
                if q_left:
                    logger.info(f"qLeft has {len(q_left)} entries")
                    logger.info(f"First qLeft entry type: {type(q_left[0])}")
                    if isinstance(q_left[0], list):
                        logger.info(f"First qLeft entry is a list with {len(q_left[0])} items")
                        if q_left[0]:
                            logger.info(f"First item in first qLeft: {q_left[0][0]}")
                    elif isinstance(q_left[0], dict):
                        logger.info(f"First qLeft entry is a dict: {q_left[0]}")

                    # Sample first 3 qLeft entries for analysis
                    for i, left_entry in enumerate(q_left[:3]):
                        logger.info(f"qLeft[{i}]: {left_entry}")

                # Check if the response uses nested qSubNodes tree format
                # (returned when bookmark/filter is applied and pivot shows full hierarchy)
                first_node = q_left[0] if q_left else None
                uses_tree_format = (
                    first_node is not None
                    and isinstance(first_node, dict)
                    and bool(first_node.get("qSubNodes"))
                )

                if uses_tree_format:
                    # Recursive tree traversal: each qLeft node is the root of a
                    # dimension tree; qSubNodes contain child dimension levels.
                    # Leaves are matched in DFS order to qData measure rows.
                    data_idx_ref = [0]
                    for top_node in q_left:
                        self._flatten_pivot_node(
                            top_node, [], dim_labels, q_data,
                            data_idx_ref, flat_rows, meas_labels
                        )
                else:
                    # Flat/sparse format: each qLeft entry is a single cell (dict)
                    # or a list of cells (one per visible dimension level).
                    # Cells reuse the previous row's value when unchanged (sparse encoding).
                    current_dim_values: Dict[int, str] = {}

                    for left_cell, data_cells in zip(q_left, q_data):
                        row: Dict = {}

                        if isinstance(left_cell, dict):
                            text = left_cell.get("qText", "")
                            elem = left_cell.get("qElemNo", -2)
                            if elem != -2 or text:
                                current_dim_values[0] = text
                            label = dim_labels[0] if dim_labels else "Dimension"
                            row[label] = current_dim_values.get(0, "")
                        elif isinstance(left_cell, list):
                            for col_idx, cell in enumerate(left_cell):
                                if isinstance(cell, dict):
                                    text = cell.get("qText", "")
                                    elem = cell.get("qElemNo", -2)
                                    if elem != -2 or text:
                                        current_dim_values[col_idx] = text
                                    label = dim_labels[col_idx] if col_idx < len(dim_labels) else f"dim{col_idx}"
                                    row[label] = current_dim_values.get(col_idx, "")

                        # Measure values
                        if isinstance(data_cells, list):
                            for col_idx, cell in enumerate(data_cells):
                                label = meas_labels[col_idx] if col_idx < len(meas_labels) else f"m{col_idx}"
                                if isinstance(cell, dict):
                                    num_val = cell.get("qNum")
                                    text_val = cell.get("qText", "")
                                    if num_val is not None and str(num_val).lower() not in ("nan", "inf", "-inf"):
                                        row[label] = num_val
                                    else:
                                        row[label] = text_val
                                else:
                                    row[label] = cell

                        flat_rows.append(row)

            # Apply client-side filters if selections were requested.
            # Match by field name (selections key) or by display label.
            if flat_rows:
                logger.info(
                    f"Row keys available for filtering: {list(flat_rows[0].keys())}"
                )

            if selections:
                for sel_field, sel_values in selections.items():
                    if not isinstance(sel_values, list):
                        sel_values = [sel_values]
                    sel_values_str = [str(v) for v in sel_values]

                    # Determine which label to filter on.
                    # Look for a dimension whose field name OR display label matches sel_field.
                    filter_label = None
                    for fld, lbl in zip(dim_fields, dim_labels):
                        if fld == sel_field or lbl == sel_field:
                            filter_label = lbl
                            break

                    before = len(flat_rows)

                    if filter_label is not None:
                        # Direct label match — standard path
                        flat_rows = [
                            r for r in flat_rows
                            if str(r.get(filter_label, "")) in sel_values_str
                        ]
                        logger.info(
                            f"Direct filter on '{filter_label}' in {sel_values_str}: "
                            f"{before} → {len(flat_rows)} rows"
                        )
                    else:
                        # sel_field is NOT a dimension column in this pivot object
                        # (e.g. YearMonth is an app-level filter field, not a displayed dim).
                        # Fallback: if sel_field == 'YearMonth', extract year-month from any
                        # date-looking field value in the row and compare.
                        # Supported date formats:
                        #   DD.MM.YYYY  (Russian)  → YYYY.MM
                        #   YYYY-MM-DD  (ISO)      → YYYY.MM
                        #   YYYY.MM.DD             → YYYY.MM
                        logger.warning(
                            f"Field '{sel_field}' not found in pivot dimensions "
                            f"{dim_fields}. Trying date-extraction fallback."
                        )

                        import re as _re
                        # Match various date formats:
                        #   M/D/YYYY  (US format, e.g. 2/1/2026 = Feb 1 2026)  → YYYY.MM
                        #   DD.MM.YYYY or DD/MM/YYYY  (Russian/European)        → YYYY.MM
                        #   YYYY-MM-DD  (ISO)                                   → YYYY.MM
                        #   YYYY.MM.DD                                          → YYYY.MM
                        _us_date  = _re.compile(r'^(\d{1,2})/(\d{1,2})/(\d{4})$')
                        _ru_date  = _re.compile(r'^(\d{1,2})[./](\d{1,2})[./](\d{4})$')
                        _iso_date = _re.compile(r'^(\d{4})-(\d{2})-\d{2}')
                        _ym_dot   = _re.compile(r'^(\d{4})\.(\d{2})')

                        def _extract_ym(val: str) -> Optional[str]:
                            s = val.strip()
                            # US M/D/YYYY  (month is group 1, day is group 2, year is group 3)
                            m = _us_date.match(s)
                            if m:
                                return f"{m.group(3)}.{m.group(1).zfill(2)}"
                            # Russian/European DD.MM.YYYY or DD/MM/YYYY
                            m = _ru_date.match(s)
                            if m:
                                return f"{m.group(3)}.{m.group(2).zfill(2)}"
                            # ISO YYYY-MM-DD
                            m = _iso_date.match(s)
                            if m:
                                return f"{m.group(1)}.{m.group(2)}"
                            # YYYY.MM or YYYY.MM.DD
                            m = _ym_dot.match(s)
                            if m:
                                return f"{m.group(1)}.{m.group(2)}"
                            return None

                        date_filtered = []
                        for r in flat_rows:
                            # Use only the FIRST date-like value found in the row
                            # (this is the primary record date, e.g. "Дата заявки").
                            # Stopping at the first date prevents false positives from
                            # secondary date fields like delivery/receipt dates.
                            matched = False
                            for v in r.values():
                                ym = _extract_ym(str(v))
                                if ym is not None:  # found a date — evaluate and stop
                                    matched = ym in sel_values_str
                                    break
                            if matched:
                                date_filtered.append(r)

                        flat_rows = date_filtered
                        logger.info(
                            f"Date-extraction fallback for '{sel_field}' in {sel_values_str}: "
                            f"{before} → {len(flat_rows)} rows"
                        )

            # Paginate the (filtered) result
            total_filtered = len(flat_rows)
            if need_all_rows:
                # Re-paginate the full filtered set
                total_pages = (total_filtered + page_size - 1) // page_size if total_filtered > 0 else 1
                offset = (page - 1) * page_size
                data_rows = flat_rows[offset: offset + page_size]
            else:
                # Already fetched exactly one page; total counts are from Qlik
                total_pages = (total_rows + page_size - 1) // page_size
                data_rows = flat_rows

            logger.info(f"Returning {len(data_rows)} rows (page {page}/{total_pages}, total {total_filtered if need_all_rows else total_rows})")
            return {
                "object_id": object_id,
                "data": data_rows,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_rows": total_filtered if need_all_rows else total_rows,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1
                }
            }

        except Exception as e:
            logger.error(f"Error in get_pivot_data for '{object_id}': {e}")
            return {"error": str(e), "details": "Error in get_pivot_data"}

    def clear_all(self, app_handle: int, locked_also: bool = False) -> bool:
        """
        Clear all selections in the app.

        Args:
            app_handle: Application handle
            locked_also: If True, clear locked selections too

        Returns:
            True if successful

        Raises:
            Exception: If clearing selections fails
        """
        try:
            logger.info("Clearing all selections")
            result = self.send_request("ClearAll", [locked_also], handle=app_handle)
            return True
        except Exception as e:
            logger.error(f"Error clearing selections: {str(e)}")
            raise
