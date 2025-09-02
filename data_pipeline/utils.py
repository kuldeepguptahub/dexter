import os, glob
from pathlib import Path
from datetime import datetime

# Helper function to get latest file
def get_latest_file(directory: Path) -> Path:
    file_list = sorted(glob.glob(os.path.join(directory, "*.parquet")))
    if not file_list:
        raise FileNotFoundError("No files found.")
    return Path(file_list[-1])

def timestamp():
    return datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

