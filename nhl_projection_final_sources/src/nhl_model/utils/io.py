from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_df(df: pd.DataFrame, base_path_without_suffix: Path, *, index: bool = False) -> None:
    base_path_without_suffix.parent.mkdir(parents=True, exist_ok=True)
    csv_path = base_path_without_suffix.with_suffix('.csv')
    parquet_path = base_path_without_suffix.with_suffix('.parquet')
    df.to_csv(csv_path, index=index)
    try:
        df.to_parquet(parquet_path, index=index)
    except Exception:
        # CSV remains the fallback if local parquet engines or mixed object columns misbehave.
        pass
