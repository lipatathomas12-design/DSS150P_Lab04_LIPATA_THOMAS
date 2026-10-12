"""Column definitions shared by every layer (order matches the source file)."""

COLS = ["ID", "Name", "Year Published", "Min Players", "Max Players", "Play Time",
        "Min Age", "Users Rated", "Rating Average", "BGG Rank",
        "Complexity Average", "Owned Users", "Mechanics", "Domains"]

TEXT_COLS = ["Name", "Mechanics", "Domains"]
NUMERIC_COLS = [c for c in COLS if c not in TEXT_COLS]
