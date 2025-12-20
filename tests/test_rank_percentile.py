import pandas as pd
import pytest

from missouri_vsr import assets


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


def test_rank_and_percentile_dense_and_exclusion():
    df = _sample_combined_df()
    value_cols = ["Total", "White"]
    mshp_mask = df["Department"].eq(assets.MSHP_DEPARTMENT_NAME)

    rank_all, pct_all = assets._rank_and_percentile(df, value_cols)
    rank_no_mshp, pct_no_mshp = assets._rank_and_percentile(
        df, value_cols, exclude_mask=mshp_mask
    )

    def _value(series: pd.Series, dept: str) -> float:
        return series.loc[df["Department"].eq(dept)].iloc[0]

    assert _value(rank_all["Total"], "Agency B") == 1
    assert _value(rank_all["Total"], "Missouri State Highway Patrol") == 2
    assert _value(rank_all["Total"], "Agency A") == 3

    assert _value(rank_all["White"], "Agency A") == 1
    assert _value(rank_all["White"], "Agency B") == 1
    assert _value(rank_all["White"], "Missouri State Highway Patrol") == 2

    assert _value(rank_no_mshp["Total"], "Agency B") == 1
    assert _value(rank_no_mshp["Total"], "Agency A") == 2
    assert pd.isna(_value(rank_no_mshp["Total"], "Missouri State Highway Patrol"))

    assert _value(rank_no_mshp["White"], "Agency A") == 1
    assert _value(rank_no_mshp["White"], "Agency B") == 1
    assert pd.isna(_value(rank_no_mshp["White"], "Missouri State Highway Patrol"))

    assert _value(pct_all["Total"], "Agency A") == pytest.approx(100 / 3, rel=1e-3)
    assert _value(pct_all["Total"], "Missouri State Highway Patrol") == pytest.approx(
        200 / 3, rel=1e-3
    )
    assert _value(pct_all["Total"], "Agency B") == pytest.approx(100.0, rel=1e-3)

    assert _value(pct_all["White"], "Agency A") == pytest.approx(100.0, rel=1e-3)
    assert _value(pct_all["White"], "Agency B") == pytest.approx(100.0, rel=1e-3)
    assert _value(pct_all["White"], "Missouri State Highway Patrol") == pytest.approx(
        100 / 3, rel=1e-3
    )

    assert _value(pct_no_mshp["Total"], "Agency A") == pytest.approx(50.0, rel=1e-3)
    assert _value(pct_no_mshp["Total"], "Agency B") == pytest.approx(100.0, rel=1e-3)
    assert pd.isna(_value(pct_no_mshp["Total"], "Missouri State Highway Patrol"))

    assert _value(pct_no_mshp["White"], "Agency A") == pytest.approx(100.0, rel=1e-3)
    assert _value(pct_no_mshp["White"], "Agency B") == pytest.approx(100.0, rel=1e-3)
    assert pd.isna(_value(pct_no_mshp["White"], "Missouri State Highway Patrol"))
