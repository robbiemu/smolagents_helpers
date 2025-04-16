import pytest
import time

from smolagents_helpers.eu_data_tool import EUDataTool


@pytest.fixture
def mock_requests(monkeypatch):
    """Fixture to patch requests.get with verify=False"""
    # Import the same requests instance that the module uses
    from smolagents_helpers.eu_data_tool import requests

    original_get = requests.get

    def patched_get(*args, **kwargs):
        kwargs["verify"] = False
        return original_get(*args, **kwargs)

    monkeypatch.setattr(requests, "get", patched_get)


@pytest.fixture
def data_tool(tmp_path):
    """Fixture providing an EUDataTool instance with isolated cache directory"""
    cache_dir = tmp_path / "eu_data_cache"
    yield EUDataTool(cache_enabled=True, cache_dir=str(cache_dir))
    # Cleanup happens automatically via tmp_path fixture


@pytest.mark.live
class TestLiveEUDataTool:
    """Live API tests for EUDataTool (will hit real data.europa.eu endpoints)"""

    TEST_DATASET_URI = "http://data.europa.eu/88u/dataset/54336a93-2478-44fc-bb78-696c77cff5c2"  # TODO - this source indicates a dataset that is served with an HTTPS certificate whos intermediate is misconfigured, causing SSL validation to fail. Likely temporary.
    TEST_KEYWORD = "earnings"
    TEST_PUBLISHER = "Central Statistics Office"

    def test_search_datasets_basic(self, data_tool):
        """Test basic dataset search functionality"""
        result = data_tool.search_datasets(keyword=self.TEST_KEYWORD, limit=5)

        assert isinstance(result, dict)
        assert "results" in result
        assert isinstance(result["results"], list)
        assert len(result["results"]) > 0

        # Verify basic structure of returned datasets
        dataset = result["results"][0]
        assert "uri" in dataset
        assert "title" in dataset
        assert isinstance(dataset["title"], str)
        assert len(dataset["title"]) > 0

    def test_search_with_filters(self, data_tool):
        """Test search with publisher filter"""
        result = data_tool.search_datasets(
            keyword=self.TEST_KEYWORD, publisher=self.TEST_PUBLISHER, limit=3
        )

        assert len(result["results"]) > 0
        for dataset in result["results"]:
            assert self.TEST_PUBLISHER.lower() in dataset.get("publisher", "").lower()

    def test_get_dataset_metadata(self, data_tool):
        """Test retrieving metadata for a known dataset"""
        metadata = data_tool.get_dataset_metadata(self.TEST_DATASET_URI)

        assert isinstance(metadata, dict)
        assert metadata.get("uri") == self.TEST_DATASET_URI
        assert "title" in metadata
        assert len(metadata["title"]) > 0
        assert "distributions" in metadata
        assert isinstance(metadata["distributions"], list)

        # Verify at least one distribution has a download URL
        assert any(
            dist.get("downloadURL") or dist.get("accessURL")
            for dist in metadata["distributions"]
        )

    def test_get_distribution_formats(self, data_tool):
        """Test getting distribution formats for a dataset"""
        formats = data_tool.get_distribution_formats(self.TEST_DATASET_URI)

        assert isinstance(formats, list)
        assert len(formats) > 0
        assert any("format" in fmt or "mediaType" in fmt for fmt in formats)

    def test_cache_functionality(self, data_tool, tmp_path):
        """Test that caching is working properly"""
        # First call - should populate cache
        start_time = time.time()
        metadata1 = data_tool.get_dataset_metadata(self.TEST_DATASET_URI)
        first_call_time = time.time() - start_time

        # Verify cache file was created
        cache_files = list(tmp_path.glob("eu_data_cache/*.json"))
        assert len(cache_files) == 1

        # Second call - should use cache
        start_time = time.time()
        metadata2 = data_tool.get_dataset_metadata(self.TEST_DATASET_URI)
        second_call_time = time.time() - start_time

        # Cached call should be significantly faster
        assert second_call_time < first_call_time / 2

        # Results should be identical
        assert metadata1 == metadata2

        # Test force refresh
        start_time = time.time()
        metadata3 = data_tool.get_dataset_metadata(
            self.TEST_DATASET_URI, force_refresh=True
        )
        third_call_time = time.time() - start_time

        # Should take similar time to first call
        assert third_call_time > first_call_time / 2
        assert metadata1 == metadata3  # Content should still match

    def test_get_dataset_content(self, data_tool, mock_requests):
        """Test downloading actual dataset content"""
        result = data_tool.get_dataset_content(
            self.TEST_DATASET_URI, preferred_formats=["CSV", "JSON"]
        )

        assert isinstance(result, dict)
        assert "content" in result
        assert "format" in result
        assert result["content"] is not None

        # Normalize the format string for comparison
        format_str = result["format"].upper()

        # Verify we got one of our preferred formats
        assert (
            "CSV" in format_str or "JSON" in format_str
        ), f"Unexpected format: {format_str}. Expected CSV or JSON"

        # Content validation based on format
        if "CSV" in format_str:
            first_line = result["content"].split("\n")[0]
            assert (
                "," in first_line or ";" in first_line
            ), "CSV content appears invalid - no delimiter found"
        elif "JSON" in format_str:
            import json

            json.loads(result["content"])  # Will raise if invalid

    def test_clear_cache(self, data_tool, tmp_path):
        """Test cache clearing functionality"""
        # Populate cache
        data_tool.get_dataset_metadata(self.TEST_DATASET_URI)
        cache_files = list(tmp_path.glob("eu_data_cache/*.json"))
        assert len(cache_files) > 0

        # Clear specific dataset cache
        data_tool.clear_cache(self.TEST_DATASET_URI)
        cache_files = list(tmp_path.glob("eu_data_cache/*.json"))
        assert len(cache_files) == 0

        # Test full cache clear
        # Populate cache again
        data_tool.get_dataset_metadata(self.TEST_DATASET_URI)
        data_tool.search_datasets(keyword=self.TEST_KEYWORD, limit=1)

        # Should have at least 2 cache files now
        cache_files = list(tmp_path.glob("eu_data_cache/*"))
        assert len(cache_files) >= 2

        # Clear everything
        data_tool.clear_cache()
        cache_files = list(tmp_path.glob("eu_data_cache/*"))
        assert len(cache_files) == 0

    def test_search_with_pagination(self, data_tool):
        """Test that pagination parameters work correctly"""
        page1 = data_tool.search_datasets(keyword=self.TEST_KEYWORD, limit=1, offset=0)
        page2 = data_tool.search_datasets(keyword=self.TEST_KEYWORD, limit=2, offset=1)

        assert len(page1["results"]) == 1
        assert len(page2["results"]) > 0

        # Verify we got different results (with small chance of collision)
        page1_uris = {d["uri"] for d in page1["results"]}
        page2_uris = {d["uri"] for d in page2["results"]}
        assert len(page1_uris.intersection(page2_uris)) == 0
