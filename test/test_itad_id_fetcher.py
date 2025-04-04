import pytest
import pandas as pd
from pathlib import Path
from pipeline.fetch_itad_id import ITADDataFetcher

@pytest.fixture
def setup_test_dirs():
    """Setup test directories and clean them after test"""
    # Create test directories
    test_root = Path("test_data")
    test_output_dir = test_root / "raw"
    test_cache_dir = test_root / "cache"
    test_error_dir = test_root / "error"
    test_log_dir = test_root / "log"
    
    # Create directories if they don't exist
    for dir_path in [test_root, test_output_dir, test_cache_dir, test_error_dir, test_log_dir]:
        dir_path.mkdir(exist_ok=True)

    test_games_path = test_root / "raw/common_ids.csv"
    
    yield {
        "output_dir": str(test_output_dir),
        "cache_dir": str(test_cache_dir),
        "error_dir": str(test_error_dir),
        "log_dir": str(test_log_dir),
        "input_file": str(test_games_path)
    }

@pytest.fixture
def fetcher(setup_test_dirs):
    """Create ITADDataFetcher instance with test directories"""
    return ITADDataFetcher(
        output_dir=setup_test_dirs["output_dir"],
        cache_dir=setup_test_dirs["cache_dir"],
        error_dir=setup_test_dirs["error_dir"],
        log_dir=setup_test_dirs["log_dir"]
    )

def test_initialization(fetcher):
    """Test if fetcher initializes correctly"""
    assert fetcher.OUTPUT_DIR.exists()
    assert fetcher.CACHE_DIR.exists()
    assert fetcher.ERROR_DIR.exists()
    assert fetcher.LOG_DIR.exists()
    assert fetcher.logger is not None
    assert fetcher.rate_limit_manager is not None

def test_fetch_single_game_info(fetcher):
    """Test fetching info for a single game"""
    # Use a known game from Valve
    test_row = {
        'appid': 10,
        'name': 'Counter-Strike'
    }
    
    # Clear any existing data
    fetcher.fetched_data = []
    
    # Fetch the game info
    fetcher.fetch_game_info(test_row)
    
    # Check results
    assert len(fetcher.fetched_data) == 1
    assert fetcher.fetched_data[0]['appid'] == 10
    assert fetcher.fetched_data[0]['name'] == 'Counter-Strike'
    assert 'itad_id' in fetcher.fetched_data[0]
    assert 'collected_at' in fetcher.fetched_data[0]
    assert str(10) in fetcher.status_cache

def test_fetch_and_save_single_game(fetcher, setup_test_dirs):
    """Test the whole process for a single game"""
    # Use a known game
    test_row = {
        'appid': 240,
        'name': 'Counter-Strike: Source'
    }
    
    # Clear existing data
    fetcher.fetched_data = []
    
    # Fetch the game info
    fetcher.fetch_game_info(test_row)
    
    # Save the data
    fetcher.save_checkpoint()
    
    # Check if data was saved
    data_path = Path(setup_test_dirs["output_dir"]) / "itad_game_ids.csv"
    assert data_path.exists()
    
    # Load the saved data
    saved_df = pd.read_csv(data_path)
    
    # Check if data is correct
    assert len(saved_df) > 0
    assert 240 in saved_df['appid'].values
    assert 'Counter-Strike: Source' in saved_df['name'].values
    assert 'itad_id' in saved_df.columns
    assert 'collected_at' in saved_df.columns

def test_run_with_real_games(fetcher, setup_test_dirs):
    """Test running the fetcher with real games"""
    # Run the fetcher with test input file
    fetcher.run(setup_test_dirs["input_file"])
    
    # Check if data file was created
    data_path = Path(setup_test_dirs["output_dir"]) / "itad_game_ids.csv"
    assert data_path.exists()
    
    # Load the saved data
    saved_df = pd.read_csv(data_path)
    
    # Check if all test games were processed
    assert len(saved_df) >= 3
    
    # Check if data contains expected games
    game_ids = saved_df['appid'].values
    assert 10 in game_ids  # Counter-Strike
    assert 240 in game_ids  # Counter-Strike: Source
    assert 440 in game_ids  # Team Fortress 2
    
    # Check if itad_ids were found (these are popular games so they should be in ITAD)
    # We don't check the exact values as they might change
    assert not saved_df[saved_df['appid'] == 10]['itad_id'].isna().any()
    assert not saved_df[saved_df['appid'] == 240]['itad_id'].isna().any()
    assert not saved_df[saved_df['appid'] == 440]['itad_id'].isna().any()

def test_cache_functionality(fetcher, setup_test_dirs):
    """Test that caching works correctly"""
    # First run
    fetcher.run(setup_test_dirs["input_file"])
    
    # Clear fetched data
    initial_fetched_count = len(fetcher.fetched_data)
    fetcher.fetched_data = []
    
    # Second run should use cache
    fetcher.run(setup_test_dirs["input_file"])
    
    # Should be empty because all games are cached
    assert len(fetcher.fetched_data) == 0
    
    # Cache file should exist
    cache_path = Path(setup_test_dirs["cache_dir"]) / "itad_status_cache.json"
    assert cache_path.exists()

def test_error_handling_with_invalid_appid(fetcher):
    """Test handling of invalid app IDs"""
    # Use an invalid app ID
    test_row = {
        'appid': 9999999,  # Very unlikely to exist
        'name': 'Non-existent Game'
    }
    
    # Clear existing data
    fetcher.fetched_data = []
    fetcher.failed_list = []
    fetcher.errored_list = []
    
    # Fetch the game info
    fetcher.fetch_game_info(test_row)
    
    # Either it will be marked as not found, or it might cause an error
    # Both cases are valid handling
    if fetcher.fetched_data:
        assert fetcher.fetched_data[0]['appid'] == 9999999
        assert fetcher.fetched_data[0]['itad_id'] is None
    else:
        assert 9999999 in fetcher.failed_list or 9999999 in fetcher.errored_list

def test_collect_multiple_batches(fetcher, setup_test_dirs):
    """Test collecting games in multiple batches"""
    # Create a larger test input with more games
    more_games_data = [
        {"appid": 10, "name": "Counter-Strike"},
        {"appid": 240, "name": "Counter-Strike: Source"},
        {"appid": 440, "name": "Team Fortress 2"},
        {"appid": 500, "name": "Left 4 Dead"},
        {"appid": 550, "name": "Left 4 Dead 2"},
        {"appid": 620, "name": "Portal 2"}
    ]
    more_games_df = pd.DataFrame(more_games_data)
    more_games_path = Path(setup_test_dirs["input_dir"]) / "more_games.csv"
    more_games_df.to_csv(more_games_path, index=False)
    
    # Run with a small batch size
    fetcher.THREAD_WORKERS = 1  # Use single thread for predictable behavior
    rows = more_games_df.to_dict('records')
    
    # Test the batch processing
    fetcher.fetch_in_parallel(rows, batch_size=2)
    
    # Check results
    data_path = Path(setup_test_dirs["output_dir"]) / "itad_game_ids.csv"
    assert data_path.exists()
    
    # Load the saved data
    saved_df = pd.read_csv(data_path)
    
    # Should have processed all games
    assert len(saved_df) >= 6