from pathlib import Path
import pandas as pd
import sys 

PROJECT_DIR = "/home/mycka/projects/amazon_books_ETL"
sys.path.insert(0, PROJECT_DIR)

from config import CONFIG


def branch_start():
    DATA_PATH = Path(CONFIG["storage"]["raw_data_dir"])
    file = list(DATA_PATH.glob("books_*.csv"))
    return file

files_list = pd.DataFrame(branch_start(), columns=["path"])
print(files_list)
    