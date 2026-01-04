import pandas as pd
import pytest
from dagster import build_op_context

from missouri_vsr.resources import LocalDirectoryResource
from missouri_vsr.assets import processed


def _sample_combined_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "agency": "Agency A",
                "year": 2023,
                "row_key": "rates-by-race--totals--all-stops",
                "metric": "All stops",
                "Total": 100,
                "White": 50,
            },
            {
                "agency": "Agency B",
                "year": 2023,
                "row_key": "rates-by-race--totals--all-stops",
                "metric": "All stops",
                "Total": 200,
                "White": 50,
            },
            {
                "agency": "Missouri State Highway Patrol",
                "year": 2023,
                "row_key": "rates-by-race--totals--all-stops",
                "metric": "All stops",
                "Total": 150,
                "White": 25,
            },
        ]
    )


def test_add_rank_percentile_rows_op():
    df = _sample_combined_df()
    context = build_op_context()
    augmented = processed.add_rank_percentile_rows(context, df)

    assert len(augmented) == len(df) * 4

    base_row_key = "rates-by-race--totals--all-stops"
    slug_suffixes = {
        row_key.replace(base_row_key, "")
        for row_key in augmented["row_key"].unique()
        if row_key.startswith(base_row_key)
    }
    assert slug_suffixes == {"", "-percentage", "-rank", "-percentile"}

    rank_row = augmented[
        (augmented["row_key"] == f"{base_row_key}-rank")
        & (augmented["agency"] == "Agency B")
    ]
    assert rank_row["Total"].iloc[0] == 1


def test_compute_statewide_baselines_op(tmp_path):
    df = _sample_combined_df()
    context = build_op_context(
        resources={"data_dir_processed": LocalDirectoryResource(path=str(tmp_path))}
    )
    baselines = processed.compute_statewide_slug_baselines(context, df)

    out_path = tmp_path / "statewide_slug_baselines.parquet"
    assert out_path.exists()

    assert set(baselines.columns) == {
        "year",
        "row_key",
        "metric",
        "count",
        "mean",
        "median",
        "count__no_mshp",
        "mean__no_mshp",
        "median__no_mshp",
    }
    assert len(baselines) == 2

    total_all = baselines[
        (baselines["metric"] == "Total")
    ].iloc[0]
    assert total_all["count"] == 3
    assert total_all["mean"] == pytest.approx(150.0, rel=1e-6)
    assert total_all["median"] == pytest.approx(150.0, rel=1e-6)

    assert total_all["count__no_mshp"] == 2
    assert total_all["mean__no_mshp"] == pytest.approx(150.0, rel=1e-6)
    assert total_all["median__no_mshp"] == pytest.approx(150.0, rel=1e-6)

    white_all = baselines[
        (baselines["metric"] == "White")
    ].iloc[0]
    assert white_all["count"] == 3
    assert white_all["mean"] == pytest.approx(41.6666667, rel=1e-6)
    assert white_all["median"] == pytest.approx(50.0, rel=1e-6)

    assert white_all["count__no_mshp"] == 2
    assert white_all["mean__no_mshp"] == pytest.approx(50.0, rel=1e-6)
    assert white_all["median__no_mshp"] == pytest.approx(50.0, rel=1e-6)
