import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    from pathlib import Path
    import pandas as pd
    import marimo as mo

    data_path = Path("data/out/statewide_year_sums.json")
    if data_path.exists():
      records = json.loads(data_path.read_text())
      df = pd.DataFrame(records)
    else:
      df = pd.DataFrame()
    return df, mo


@app.cell
def _(mo):
    scale_picker = mo.ui.dropdown(
      options=["linear", "sqrt", "symlog"],
      value="sqrt",
      label="Y scale"
    )
    scale_picker
    return (scale_picker,)


@app.cell
def _(df, mo):
    row_key_picker = mo.ui.multiselect(
      options=sorted(df["row_key"].dropna().unique()) if not df.empty else [],
      value=[],
      label="Row keys"
    )
    row_key_picker
    return (row_key_picker,)


@app.cell
def _(df, mo, row_key_picker, scale_picker):
    import altair as alt

    value_cols = [
      "Total",
      "White",
      "Black",
      "Hispanic",
      "Native American",
      "Asian",
      "Other",
    ]

    selected = row_key_picker.value

    if not selected:
      view = mo.md("Pick one or more row_keys to render charts.")
    else:
      filtered = df[df["row_key"].isin(selected)][["row_key", "year",
    *value_cols]].copy()
      filtered = filtered.dropna(subset=["year"])
      filtered["year"] = filtered["year"].astype(int)

      long = filtered.melt(
          id_vars=["row_key", "year"],
          value_vars=value_cols,
          var_name="series",
          value_name="value",
      ).dropna(subset=["value"])

      scale_type = scale_picker.value
      y_scale = alt.Scale(type=scale_type)

      chart = (
          alt.Chart(long)
          .mark_line(point=True)
          .encode(
              x=alt.X("year:O", title="Year"),
              y=alt.Y("value:Q", title="Value", scale=y_scale),
              color=alt.Color("series:N", title="Series"),
              facet=alt.Facet("row_key:N", columns=2),
              tooltip=["row_key:N", "series:N", "year:O", "value:Q"],
          )
          .properties(width=500, height=350)
          .resolve_scale(y="independent")
      )
      view = chart

    view
    return


@app.cell
def _(df):
    df
    return


if __name__ == "__main__":
    app.run()
