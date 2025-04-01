import logging
from pathlib import Path
from contextlib import contextmanager
from time import time

def setup_logger(name: str, log_dir="logs", level=logging.INFO):
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    log_file = log_dir_path / f"{name}.log"

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

        # 파일 핸들러
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
    
# from utils.logger import setup_logger
# logger = setup_logger("fetch_steam_list")

# logger.info("게임 리스트 수집 시작")