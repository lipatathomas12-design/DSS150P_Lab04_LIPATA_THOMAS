import pandas as pd
import pytest

from src.common import load_config
from src.constants import COLS


@pytest.fixture
def cfg():
    return load_config()


@pytest.fixture
def raw_df():
    """Tiny raw-layer frame (all text) with the quirks the real file has."""
    rows = [
        ["1", "A", "2000", "2", "4", "60", "10", "100", "7,5", "5", "2,5", "300", "Dice Rolling, Trading", "Strategy Games"],
        ["2", "B", "1999", "0", "0", "0", "0", "50", "5,2", "9", "0", "20", "", ""],
        ["2", "B dup", "1999", "1", "2", "30", "8", "50", "5,2", "9", "2", "20", "x", "y"],   # duplicate ID
        ["", "No key", "2001", "1", "2", "30", "8", "50", "6,0", "9", "2", "20", "x", "y"],   # missing ID
        ["3", "C", "2010", "1", "1", "45", "12", "10", "8,1", "2", "3,1", "", "nan", "nan"],
    ]
    df = pd.DataFrame(rows, columns=COLS)
    df.insert(0, "source_row_number", range(1, len(df) + 1))
    return df