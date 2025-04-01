import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch

from pipeline.fetch_steam_list import SteamListFetcher

@pytest.fixture
def fetcher():
    # Create a test output directory in test_data
    test_output_dir = Path("test_data/raw")
    test_output_dir.mkdir(exist_ok=True)
    test_cache_dir = Path("test_data/cache")
    test_cache_dir.mkdir(exist_ok=True)
    test_error_dir = Path("test_data/error")
    test_error_dir.mkdir(exist_ok=True)

    return SteamListFetcher(
        output_dir=str(test_output_dir), 
        cache_dir=str(test_cache_dir), 
        error_dir=str(test_error_dir),
        steamcharts_games=10  # Limit pages for testing
    )

def test_initialization(fetcher):
    """Test basic initialization of SteamListFetcher"""
    assert fetcher.OUTPUT_DIR.exists()
    assert fetcher.CACHE_DIR.exists()
    assert fetcher.ERROR_DIR.exists()
    assert fetcher.logger is not None

def test_scrape_steamcharts_parallel(fetcher):
    """Test parallel scraping of SteamCharts"""
    fetcher.scrape_steamcharts_parallel()

    # Check output file exists
    assert fetcher.steamcharts_path.exists()

    # Load scraped data
    df = pd.read_csv(fetcher.steamcharts_path)
    
    # Assertions
    assert len(df) > 0
    assert set(df.columns) == {"rank", "appid", "game_name"}
    assert df["appid"].notna().all()
    assert df["game_name"].notna().all()

def test_fetch_all_apps(fetcher):
    """Test fetching all Steam apps"""
    fetcher.fetch_all_apps()

    # Check output file exists
    assert fetcher.all_apps_path.exists()

    # Load apps data
    df = pd.read_csv(fetcher.all_apps_path)
    
    # Assertions
    assert len(df) > 0
    assert set(df.columns) == {"appid", "name"}
    assert df["appid"].notna().all()

def test_filter_common_ids(fetcher):
    """Test filtering common IDs between SteamCharts and all apps"""
    # First, ensure we have data to work with
    fetcher.scrape_steamcharts_parallel()
    fetcher.fetch_all_apps()
    
    # Run filtering
    fetcher.filter_common_ids()

    # Check output file exists
    assert fetcher.common_ids_path.exists()

    # Load common IDs
    df = pd.read_csv(fetcher.common_ids_path)
    
    # Assertions
    assert len(df) > 0
    assert set(df.columns) == {"appid", "name"}
    assert df["appid"].notna().all()

def test_error_handling(fetcher):
    """Test error handling and logging mechanisms"""
    # Simulate an error scenario
    with patch('pipeline.fetch_steam_list.requests.get') as mock_get:
        mock_get.side_effect = Exception("Simulated error")
        
        try:
            fetcher.fetch_all_apps()
        except Exception:
            pass

    # Check that errors are logged
    log_files = list(fetcher.LOG_DIR.glob('*.log'))
    assert len(log_files) > 0

def test_run_full_process(fetcher):
    """Test the complete run process"""
    fetcher.run()

    # Check all expected files are created
    assert fetcher.steamcharts_path.exists()
    assert fetcher.all_apps_path.exists()
    assert fetcher.common_ids_path.exists()
    assert fetcher.CACHE_FILE.exists()