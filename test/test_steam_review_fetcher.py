import pytest
import pandas as pd
from pathlib import Path
from pipeline.fetch_steam_review import SteamReviewFetcher

@pytest.fixture
def fetcher():
    # Create a test output directory in test_data
    test_output_dir = Path("test_data/raw")
    test_output_dir.mkdir(exist_ok=True)
    test_cache_dir = Path("test_data/cache")
    test_cache_dir.mkdir(exist_ok=True)
    test_error_dir = Path("test_data/error")
    test_error_dir.mkdir(exist_ok=True)
    return SteamReviewFetcher(output_dir=str(test_output_dir), cache_dir=str(test_cache_dir), error_dir=str(test_error_dir))

def test_initialization(fetcher):
    assert fetcher.OUTPUT_DIR.exists()
    assert fetcher.CACHE_DIR.exists()
    assert fetcher.ERROR_DIR.exists()
    assert fetcher.logger is not None

def test_fetch_review_data(fetcher):
    # Use mock CSV from test_data
    input_csv_path = "test_data/raw/steam_app_ids.csv"
    
    # Run the fetcher
    fetcher.run(input_csv_path)
    
    # Check if results were saved
    assert fetcher.reviews_df_path.exists()
    
    # Load the saved reviews
    reviews_df = pd.read_csv(fetcher.reviews_df_path)
    
    # Basic assertions
    assert len(reviews_df) > 0
    assert 'appid' in reviews_df.columns
    assert 'num_reviews' in reviews_df.columns

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
    assert fetcher.reviews_df_path.exists()