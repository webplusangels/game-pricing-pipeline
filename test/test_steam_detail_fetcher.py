import pytest
import pandas as pd
from pathlib import Path
from pipeline.fetch_steam_detail import SteamDetailFetcher

@pytest.fixture
def fetcher():
    # Create a test output directory in test_data
    test_output_dir = Path("test_data/raw")
    test_output_dir.mkdir(exist_ok=True)
    test_cache_dir = Path("test_data/cache")
    test_cache_dir.mkdir(exist_ok=True)
    test_error_dir = Path("test_data/error")
    test_error_dir.mkdir(exist_ok=True)
    return SteamDetailFetcher(output_dir=str(test_output_dir), cache_dir=str(test_cache_dir), error_dir=str(test_error_dir))

def test_initialization(fetcher):
    assert fetcher.OUTPUT_DIR.exists()
    assert fetcher.CACHE_DIR.exists()
    assert fetcher.ERROR_DIR.exists()
    assert fetcher.logger is not None

def test_fetch_detail_data(fetcher):
    # Use mock CSV from test_data
    input_csv_path = "test_data/raw/steam_app_ids.csv"
    
    # Run the fetcher
    fetcher.run(input_csv_path)
    
    # Check if results were saved
    assert fetcher.original_df_path.exists()
    assert fetcher.parsed_df_path.exists()
    
    # Load the saved reviews
    original_df = pd.read_csv(fetcher.original_df_path)
    parsed_df = pd.read_csv(fetcher.parsed_df_path)
    
    # Basic assertions
    assert len(original_df) > 0
    assert len(parsed_df) > 0
    assert 'steam_appid' in original_df.columns
    assert 'appid' in parsed_df.columns
    assert 'name' in parsed_df.columns
    assert 'short_description' in parsed_df.columns
    assert 'is_free' in parsed_df.columns
    assert 'release_date' in parsed_df.columns
    assert 'header_image' in parsed_df.columns
    assert 'developer' in parsed_df.columns
    assert 'publisher' in parsed_df.columns
    assert 'initial_price' in parsed_df.columns
    assert 'final_price' in parsed_df.columns
    assert 'discount_percent' in parsed_df.columns
    assert 'categories' in parsed_df.columns
    assert 'genres' in parsed_df.columns

def test_error_handling(fetcher):
    # Check error handling mechanisms
    assert hasattr(fetcher, 'failed_list')
    assert hasattr(fetcher, 'errored_list')

def test_checkpoint_saving(fetcher):
    # Simulate data collection
    input_csv_path = "test_data/raw/steam_app_ids.csv"
    fetcher.run(input_csv_path)
    
    # Check if checkpoint files exist
    assert fetcher.CACHE_FILE.exists()
    assert fetcher.original_df_path.exists()