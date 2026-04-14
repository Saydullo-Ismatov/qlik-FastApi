"""Tests for get_object_data pagination logic in AppRepository.

Verifies that:
- page_size=5, page=2 contains data that matches rows 6-10 of page_size=10, page=1
- Selections (Qlik-side filtering) work correctly with pagination
- Edge cases: last page with fewer rows, empty results, single row
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call

from src.api.repositories.app_repository import AppRepository


# --- Helpers to build mock engine responses ---

def make_layout_response(total_rows, dim_count, meas_count, dim_labels=None, meas_labels=None, dim_fields=None):
    """Build a fake GetLayout response."""
    dim_infos = []
    for i in range(dim_count):
        label = (dim_labels[i] if dim_labels else f"Dim_{i}")
        field = (dim_fields[i] if dim_fields else label)
        dim_infos.append({
            "qFallbackTitle": label,
            "qGroupFieldDefs": [field],
        })

    meas_infos = []
    for i in range(meas_count):
        label = (meas_labels[i] if meas_labels else f"Meas_{i}")
        meas_infos.append({
            "qFallbackTitle": label,
        })

    return {
        "qLayout": {
            "qHyperCube": {
                "qSize": {"qcy": total_rows, "qcx": dim_count + meas_count},
                "qDimensionInfo": dim_infos,
                "qMeasureInfo": meas_infos,
            }
        }
    }


def make_property_tree(dim_count, meas_count, meas_expressions=None):
    """Build a fake GetFullPropertyTree response."""
    measures = []
    for i in range(meas_count):
        expr = (meas_expressions[i] if meas_expressions else f"Sum(Field_{i})")
        measures.append({
            "qDef": {"qDef": expr, "qLabel": f"Meas_{i}"},
        })
    return {
        "qPropEntry": {
            "qProperty": {
                "qHyperCubeDef": {
                    "qDimensions": [{"qDef": {"qFieldDefs": [f"Dim_{i}"]}} for i in range(dim_count)],
                    "qMeasures": measures,
                    "qColumnOrder": list(range(dim_count + meas_count)),
                }
            }
        }
    }


def make_matrix_rows(start_idx, count, dim_count, meas_count):
    """Build qMatrix rows with predictable values.

    Row i has:
      - dims: "dim_j_val_{start_idx + i}" for j in range(dim_count)
      - measures: qNum = (start_idx + i) * 100 + j for j in range(meas_count)
    """
    matrix = []
    for i in range(count):
        row_num = start_idx + i
        cells = []
        for j in range(dim_count):
            cells.append({"qText": f"dim_{j}_val_{row_num}", "qNum": float(row_num)})
        for j in range(meas_count):
            cells.append({"qText": str(row_num * 100 + j), "qNum": float(row_num * 100 + j)})
        matrix.append(cells)
    return matrix


def make_data_page_response(matrix):
    """Wrap a matrix in a GetHyperCubeData response."""
    return {"qDataPages": [{"qMatrix": matrix}]}


# --- Fixtures ---

@pytest.fixture
def mock_engine():
    """Create a mock engine client."""
    engine = MagicMock()
    engine.connect = Mock()
    engine.disconnect = Mock()
    engine.open_doc = Mock(return_value={"qReturn": {"qHandle": 1, "qType": "Doc"}})
    engine.select_values = Mock()
    engine.clear_all = Mock()
    return engine


@pytest.fixture
def mock_repo_client():
    """Create a mock repository client."""
    return MagicMock()


@pytest.fixture
def app_repo(mock_repo_client, mock_engine):
    """Create AppRepository with mocked clients."""
    repo = AppRepository(
        repository_client=mock_repo_client,
        engine_client=mock_engine
    )
    return repo


# --- Tests ---

class TestGetObjectDataPagination:
    """Test pagination consistency for get_object_data (regular hypercube path)."""

    TOTAL_ROWS = 23
    DIM_COUNT = 2
    MEAS_COUNT = 1

    def _setup_engine_for_regular_hypercube(self, mock_engine, total_rows=None):
        """Configure mock engine for the regular (non-pivot) hypercube path."""
        total = total_rows or self.TOTAL_ROWS

        # GetObject response
        mock_engine.send_request = Mock()

        def send_request_side_effect(method, *args, handle=-1, **kwargs):
            if method == "GetObject":
                return {"qReturn": {"qHandle": 2, "qType": "GenericObject"}}
            elif method == "GetLayout":
                return make_layout_response(total, self.DIM_COUNT, self.MEAS_COUNT)
            elif method == "GetFullPropertyTree":
                return make_property_tree(self.DIM_COUNT, self.MEAS_COUNT)
            elif method == "GetHyperCubeData":
                # args[0] is the path, args[1] is the page spec list
                page_spec = args[1][0] if len(args) > 1 else args[0][1][0]
                q_top = page_spec["qTop"]
                q_height = page_spec["qHeight"]
                # Clamp to available rows
                actual_rows = min(q_height, max(0, total - q_top))
                matrix = make_matrix_rows(q_top, actual_rows, self.DIM_COUNT, self.MEAS_COUNT)
                return make_data_page_response(matrix)
            elif method == "DestroySessionObject":
                return {}
            return {}

        mock_engine.send_request.side_effect = send_request_side_effect

    def _get_all_row_values(self, result):
        """Extract a list of identifying values from result data rows."""
        return [row.get("Dim_0") for row in result["data"]]

    def test_page2_size5_matches_page1_size10_rows_6_to_10(self, app_repo, mock_engine):
        """Page 2 with page_size=5 should contain the same rows as rows 6-10 of page 1 with page_size=10."""
        self._setup_engine_for_regular_hypercube(mock_engine)

        # Fetch page 1 with page_size=10
        result_10 = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=10, filters={}, selections={}, variables={}
        )

        self._setup_engine_for_regular_hypercube(mock_engine)

        # Fetch page 2 with page_size=5
        result_p2_s5 = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=2, page_size=5, filters={}, selections={}, variables={}
        )

        rows_10 = self._get_all_row_values(result_10)
        rows_p2_s5 = self._get_all_row_values(result_p2_s5)

        # Rows 6-10 from page_size=10 (indices 5-9) should equal page 2 of page_size=5
        assert rows_10[5:10] == rows_p2_s5, (
            f"page_size=10 rows[5:10]={rows_10[5:10]} != page_size=5 page2={rows_p2_s5}"
        )

    def test_page1_size5_matches_page1_size10_rows_1_to_5(self, app_repo, mock_engine):
        """Page 1 with page_size=5 should contain the same rows as the first 5 rows of page_size=10."""
        self._setup_engine_for_regular_hypercube(mock_engine)

        result_10 = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=10, filters={}, selections={}, variables={}
        )

        self._setup_engine_for_regular_hypercube(mock_engine)

        result_5 = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=5, filters={}, selections={}, variables={}
        )

        rows_10 = self._get_all_row_values(result_10)
        rows_5 = self._get_all_row_values(result_5)

        assert rows_10[:5] == rows_5

    def test_all_pages_cover_all_rows(self, app_repo, mock_engine):
        """Paginating through all pages with page_size=5 should cover all 23 rows exactly once."""
        page_size = 5
        all_rows = []

        for page_num in range(1, 6):  # 23 rows / 5 = 5 pages (last page has 3 rows)
            self._setup_engine_for_regular_hypercube(mock_engine)
            result = app_repo.get_object_data(
                app_id="test-app", object_id="test-obj",
                page=page_num, page_size=page_size, filters={}, selections={}, variables={}
            )
            all_rows.extend(self._get_all_row_values(result))

        # Should have exactly 23 unique rows
        assert len(all_rows) == self.TOTAL_ROWS
        # Each row should be unique (no duplicates from pagination boundaries)
        assert len(set(all_rows)) == self.TOTAL_ROWS

    def test_last_page_partial(self, app_repo, mock_engine):
        """Last page should have fewer rows when total isn't evenly divisible."""
        self._setup_engine_for_regular_hypercube(mock_engine)

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=5, page_size=5, filters={}, selections={}, variables={}
        )

        # 23 total rows, page 5 of size 5 = rows 21-23, only 3 rows
        assert len(result["data"]) == 3
        assert result["pagination"]["has_next"] is False
        assert result["pagination"]["has_previous"] is True

    def test_pagination_metadata(self, app_repo, mock_engine):
        """Pagination metadata should be correct."""
        self._setup_engine_for_regular_hypercube(mock_engine)

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=5, filters={}, selections={}, variables={}
        )

        pagination = result["pagination"]
        assert pagination["page"] == 1
        assert pagination["total_rows"] == self.TOTAL_ROWS
        assert pagination["has_next"] is True
        assert pagination["has_previous"] is False

    def test_empty_result(self, app_repo, mock_engine):
        """Object with 0 rows and 0 dimensions should return empty data.

        Note: 0 rows with dimensions > 0 triggers pivot detection path,
        so we test with 0 dimensions for the regular hypercube empty case.
        """
        total_rows = 0

        def send_request_side_effect(method, *args, handle=-1, **kwargs):
            if method == "GetObject":
                return {"qReturn": {"qHandle": 2, "qType": "GenericObject"}}
            elif method == "GetLayout":
                # 0 dims and 0 rows avoids pivot detection
                return make_layout_response(0, 0, 1)
            elif method == "GetFullPropertyTree":
                return make_property_tree(0, 1)
            elif method == "GetHyperCubeData":
                return make_data_page_response([])
            return {}

        mock_engine.send_request.side_effect = send_request_side_effect

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=10, filters={}, selections={}, variables={}
        )

        assert len(result["data"]) == 0
        assert result["pagination"]["total_rows"] == 0

    def test_single_row(self, app_repo, mock_engine):
        """Object with exactly 1 row should work correctly."""
        self._setup_engine_for_regular_hypercube(mock_engine, total_rows=1)

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=5, filters={}, selections={}, variables={}
        )

        assert len(result["data"]) == 1
        assert result["pagination"]["has_next"] is False
        assert result["pagination"]["has_previous"] is False

    def test_page_size_equals_total(self, app_repo, mock_engine):
        """When page_size equals total_rows, one page should contain everything."""
        self._setup_engine_for_regular_hypercube(mock_engine, total_rows=5)

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=5, filters={}, selections={}, variables={}
        )

        assert len(result["data"]) == 5
        assert result["pagination"]["total_pages"] == 1
        assert result["pagination"]["has_next"] is False


class TestGetObjectDataWithSelections:
    """Test that selections use Qlik-side filtering (not client-side fetch-all)."""

    def test_selections_call_select_values(self, app_repo, mock_engine):
        """When selections are provided, select_values should be called on the engine."""
        total_rows = 10

        def send_request_side_effect(method, *args, handle=-1, **kwargs):
            if method == "GetObject":
                return {"qReturn": {"qHandle": 2, "qType": "GenericObject"}}
            elif method == "GetLayout":
                return make_layout_response(total_rows, 2, 1)
            elif method == "GetFullPropertyTree":
                return make_property_tree(2, 1)
            elif method == "GetHyperCubeData":
                page_spec = args[1][0] if len(args) > 1 else args[0][1][0]
                q_top = page_spec["qTop"]
                q_height = page_spec["qHeight"]
                actual = min(q_height, max(0, total_rows - q_top))
                return make_data_page_response(make_matrix_rows(q_top, actual, 2, 1))
            return {}

        mock_engine.send_request.side_effect = send_request_side_effect

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=1, page_size=5,
            filters={},
            selections={"PRCTR": ["1203"]},
            variables={}
        )

        # select_values should have been called for the selection
        mock_engine.select_values.assert_called()
        call_args = mock_engine.select_values.call_args
        assert call_args[0][1] == "PRCTR"  # field name
        assert call_args[0][2] == ["1203"]  # values

    def test_no_filters_means_no_full_fetch(self, app_repo, mock_engine):
        """Without filters, only the requested page should be fetched (not all rows)."""
        total_rows = 100

        fetched_pages = []

        def send_request_side_effect(method, *args, handle=-1, **kwargs):
            if method == "GetObject":
                return {"qReturn": {"qHandle": 2, "qType": "GenericObject"}}
            elif method == "GetLayout":
                return make_layout_response(total_rows, 2, 1)
            elif method == "GetFullPropertyTree":
                return make_property_tree(2, 1)
            elif method == "GetHyperCubeData":
                page_spec = args[1][0] if len(args) > 1 else args[0][1][0]
                fetched_pages.append(page_spec)
                q_top = page_spec["qTop"]
                q_height = page_spec["qHeight"]
                actual = min(q_height, max(0, total_rows - q_top))
                return make_data_page_response(make_matrix_rows(q_top, actual, 2, 1))
            return {}

        mock_engine.send_request.side_effect = send_request_side_effect

        result = app_repo.get_object_data(
            app_id="test-app", object_id="test-obj",
            page=2, page_size=5,
            filters={}, selections={}, variables={}
        )

        # Should have fetched exactly 1 page of data
        assert len(fetched_pages) == 1
        # Should have started at row 5 (page 2, 0-indexed)
        assert fetched_pages[0]["qTop"] == 5
        # Should have requested only 5 rows
        assert fetched_pages[0]["qHeight"] == 5

        assert len(result["data"]) == 5
