import marimo as mo

app = mo.App(width="full")


@app.cell
def __():
    import json
    from pathlib import Path

    import pandas as pd

    data_path = Path("data/out/statewide_year_sums.json")
    if data_path.exists():
        records = json.loads(data_path.read_text())
        df = pd.DataFrame(records)
    else:
        df = pd.DataFrame()
    return df, data_path


@app.cell
def __(df, mo, data_path):
    if df.empty:
        return mo.md(
            f"No data found at `{data_path}`. Materialize `statewide_year_sums_json` first."
        )
    return mo.ui.dataframe(df)


if __name__ == "__main__":
    app.run()
