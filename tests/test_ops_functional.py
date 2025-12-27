import pandas as pd
import pytest
from dagster import build_op_context

from missouri_vsr.resources import LocalDirectoryResource
from missouri_vsr.assets import processed


def _sample_combined_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Department": "Agency A",
                "year": 2023,
                "slug": "rates--totals--all-stops",
                "Total": 100,
                "White": 50,
            },
            {
                "Department": "Agency B",
                "year": 2023,
                "slug": "rates--totals--all-stops",
                "Total": 200,
                "White": 50,
            },
            {
                "Department": "Missouri State Highway Patrol",
                "year": 2023,
                "slug": "rates--totals--all-stops",
                "Total": 150,
                "White": 25,
            },
        ]
    )


def test_add_rank_percentile_rows_op():
    df = _sample_combined_df()
    context = build_op_context()
    augmented = processed.add_rank_percentile_rows(context, df)

    assert len(augmented) == len(df) * 5

    base_slug = "rates--totals--all-stops"
    slug_suffixes = {
        slug.replace(base_slug, "")
        for slug in augmented["slug"].unique()
        if slug.startswith(base_slug)
    }
    assert slug_suffixes == {
        "",
        "-rank",
        "-percentile",
        "-rank-no-mshp",
        "-percentile-no-mshp",
    }

    rank_row = augmented[
        (augmented["slug"] == f"{base_slug}-rank")
        & (augmented["Department"] == "Agency B")
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

    assert set(baselines["scope"]) == {"all", "no_mshp"}
    assert len(baselines) == 4

    total_all = baselines[
        (baselines["metric"] == "Total") & (baselines["scope"] == "all")
    ].iloc[0]
    assert total_all["count"] == 3
    assert total_all["mean"] == pytest.approx(150.0, rel=1e-6)
    assert total_all["median"] == pytest.approx(150.0, rel=1e-6)

    total_no = baselines[
        (baselines["metric"] == "Total") & (baselines["scope"] == "no_mshp")
    ].iloc[0]
    assert total_no["count"] == 2
    assert total_no["mean"] == pytest.approx(150.0, rel=1e-6)
    assert total_no["median"] == pytest.approx(150.0, rel=1e-6)

    white_all = baselines[
        (baselines["metric"] == "White") & (baselines["scope"] == "all")
    ].iloc[0]
    assert white_all["count"] == 3
    assert white_all["mean"] == pytest.approx(41.6666667, rel=1e-6)
    assert white_all["median"] == pytest.approx(50.0, rel=1e-6)

    white_no = baselines[
        (baselines["metric"] == "White") & (baselines["scope"] == "no_mshp")
    ].iloc[0]
    assert white_no["count"] == 2
    assert white_no["mean"] == pytest.approx(50.0, rel=1e-6)
    assert white_no["median"] == pytest.approx(50.0, rel=1e-6)
