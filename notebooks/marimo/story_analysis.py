import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


# ── data loading & setup ─────────────────────────────────────────────────────


@app.cell
def _():
    import warnings
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import pandas as pd

    warnings.filterwarnings("ignore")

    RACE_COLS = ["Total", "White", "Black", "Hispanic", "Native American", "Asian"]
    STORY_COLS = ["White", "Black", "Hispanic"]

    # gold for Hispanic so it pops against gray/black
    RACE_COLORS = alt.Scale(
        domain=["White", "Black", "Hispanic"],
        range=["#888888", "#1a1a1a", "#d4a017"],
    )

    DATA_DIR = Path("data/out")

    def load_csv(name: str) -> pd.DataFrame:
        for directory in [DATA_DIR, Path("/tmp")]:
            path = directory / name
            if path.exists():
                return pd.read_csv(path, low_memory=False)
        return pd.DataFrame()

    df = load_csv("vsr_stats.csv")
    comments = load_csv("agency_comments.csv")

    def rows(key: str) -> pd.DataFrame:
        sub = df[df["row_key"] == key].copy()
        for c in RACE_COLS:
            if c in sub.columns:
                sub[c] = pd.to_numeric(sub[c], errors="coerce")
        return sub

    def by_year(key: str) -> pd.DataFrame:
        return rows(key).groupby("year")[RACE_COLS].sum()

    def pct(num, denom):
        return (num / denom * 100).round(2)

    # base denominators used throughout
    stops_yr = by_year("rates-by-race--totals--all-stops")
    searches_yr = by_year("rates-by-race--totals--searches")
    arrests_yr = by_year("number-of-stops-by-race--stop-outcome--arrests")
    contraband_yr = by_year("rates-by-race--totals--contraband")

    return (
        RACE_COLORS,
        RACE_COLS,
        STORY_COLS,
        alt,
        arrests_yr,
        by_year,
        comments,
        contraband_yr,
        df,
        mo,
        pd,
        pct,
        rows,
        searches_yr,
        stops_yr,
    )


# ── chart helper ─────────────────────────────────────────────────────────────


@app.cell
def _(RACE_COLORS, STORY_COLS, alt, pd):
    def make_trend_chart(
        data: pd.DataFrame,
        title: str,
        ylabel: str,
        cols: list[str] | None = None,
        width: int = 620,
        height: int = 340,
        fmt: str = ".1f",
    ):
        """Reusable Altair line chart with labeled endpoints."""
        if cols is None:
            cols = STORY_COLS
        long = (
            data[cols]
            .reset_index()
            .melt(id_vars="year", var_name="Race", value_name="value")
        )
        long["year"] = long["year"].astype(int)

        line = (
            alt.Chart(long)
            .mark_line(point=True, strokeWidth=2.5)
            .encode(
                x=alt.X("year:O", title="Year"),
                y=alt.Y("value:Q", title=ylabel),
                color=alt.Color("Race:N", scale=RACE_COLORS, legend=alt.Legend(title=None)),
                tooltip=[
                    alt.Tooltip("Race:N"),
                    alt.Tooltip("year:O", title="Year"),
                    alt.Tooltip("value:Q", title=ylabel, format=fmt),
                ],
            )
        )

        # label the last point on each line
        last_year = long["year"].max()
        labels = (
            alt.Chart(long[long["year"] == last_year])
            .mark_text(align="left", dx=8, fontSize=12, fontWeight="bold")
            .encode(
                x=alt.X("year:O"),
                y=alt.Y("value:Q"),
                text=alt.Text("value:Q", format=fmt),
                color=alt.Color("Race:N", scale=RACE_COLORS, legend=None),
            )
        )

        return (
            (line + labels)
            .properties(width=width, height=height, title=title)
            .configure_view(strokeWidth=0)
        )

    return (make_trend_chart,)


# ═══════════════════════════════════════════════════════════════════════════════
# STORY HEADER
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        # Hispanic traffic stops in Missouri are up 57 percent since 2020. For white drivers, the increase was 10.

        Analysis of Missouri's Vehicle Stop Report (VSR) data, 2020–2024. Each
        section below corresponds to a story pitch. All numbers are computed
        from the state's own mandatory reporting data.

        ---
        """
    )
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 1. THE LEAD — the 57% surge
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 1. The lead: the 57-percent surge

        We index every group's stop count to 2020 = 100. The Hispanic line
        detaches from the pack after 2021 and never comes back.
        """
    )
    return


@app.cell
def _(make_trend_chart, mo, stops_yr):
    _indexed = (stops_yr / stops_yr.iloc[0] * 100).round(1)

    _chart = make_trend_chart(
        _indexed,
        "Traffic stops indexed to 2020",
        "Index (2020 = 100)",
    )

    mo.hstack([
        _chart,
        mo.md(f"""
**Raw numbers:**
- Hispanic: {stops_yr['Hispanic'].iloc[0]:,.0f} → {stops_yr['Hispanic'].iloc[-1]:,.0f} (+57%)
- White: {stops_yr['White'].iloc[0]:,.0f} → {stops_yr['White'].iloc[-1]:,.0f} (+10%)
- Black: {stops_yr['Black'].iloc[0]:,.0f} → {stops_yr['Black'].iloc[-1]:,.0f} (+4%)
        """),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 2. THE FLIP — search rate reversal
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 2. The flip: search rate reversal

        In 2020, Hispanic drivers were searched *less* often than white drivers.
        By 2021 that had reversed, and the gap has widened every year since.
        """
    )
    return


@app.cell
def _(make_trend_chart, mo, pct, searches_yr, stops_yr):
    _search_rate = pct(searches_yr, stops_yr)
    _search_rate["Ratio (H/W)"] = (_search_rate["Hispanic"] / _search_rate["White"]).round(2)

    _chart = make_trend_chart(
        _search_rate,
        "Search rate by race (% of stops)",
        "Search rate %",
    )

    mo.hstack([
        _chart,
        mo.md(f"""
**The crossover:**
- 2020: Hispanic {_search_rate.loc[2020, 'Hispanic']}% vs White {_search_rate.loc[2020, 'White']}% → ratio **{_search_rate.loc[2020, 'Ratio (H/W)']}**
- 2024: Hispanic {_search_rate.loc[2024, 'Hispanic']}% vs White {_search_rate.loc[2024, 'White']}% → ratio **{_search_rate.loc[2024, 'Ratio (H/W)']}**

The ratio went from below 1 (searched *less*) to 1.37 (searched *more*).
        """),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 3. STOPPED MORE, FOUND LESS — contraband hit rate
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 3. Stopped more, found less: contraband hit rate

        The classic test for pretextual enforcement. If you're searching people
        for good reasons, you should find something. Hispanic hit rate was 35%
        in 2020 — *higher than white*. By 2023 it had fallen to 16%.
        """
    )
    return


@app.cell
def _(contraband_yr, make_trend_chart, pct, searches_yr):
    _hit_rate = pct(contraband_yr, searches_yr)

    make_trend_chart(
        _hit_rate,
        "Contraband hit rate (% of searches finding contraband)",
        "Hit rate %",
    )
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 4. THE ARREST GAP
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 4. The arrest gap

        Hispanic arrest rate during stops: 4.80% (2020) → 6.17% (2024).
        White arrest rate: 3.70% → 3.38%. The lines are diverging.
        """
    )
    return


@app.cell
def _(arrests_yr, make_trend_chart, mo, pct, stops_yr):
    _arrest_rate = pct(arrests_yr, stops_yr)
    _arrest_rate["Hisp/White"] = (_arrest_rate["Hispanic"] / _arrest_rate["White"]).round(2)
    _arrest_rate["Black/White"] = (_arrest_rate["Black"] / _arrest_rate["White"]).round(2)

    _chart = make_trend_chart(
        _arrest_rate,
        "Arrest rate by race (% of stops)",
        "Arrest rate %",
    )

    mo.hstack([
        _chart,
        mo.md(f"""
**Hispanic-to-white arrest ratio:**
- 2020: **{_arrest_rate.loc[2020, 'Hisp/White']}x**
- 2024: **{_arrest_rate.loc[2024, 'Hisp/White']}x**

**Black-to-white arrest ratio:**
- 2020: **{_arrest_rate.loc[2020, 'Black/White']}x**
- 2024: **{_arrest_rate.loc[2024, 'Black/White']}x**
        """),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 5. EQUIPMENT VIOLATIONS
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 5. Equipment violations: the pretext door

        Equipment stops (broken taillight, tinted windows) are the most
        officer-discretionary reason for a stop. Hispanic equipment stops
        are up 75% since 2020. White: up 6%.
        """
    )
    return


@app.cell
def _(by_year, make_trend_chart):
    _equip = by_year("number-of-stops-by-race--reason-for-stop--equpiment")
    _equip_idx = (_equip / _equip.iloc[0] * 100).round(1)

    make_trend_chart(
        _equip_idx,
        "Equipment violation stops indexed to 2020",
        "Index (2020 = 100)",
    )
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 6. INVESTIGATIVE STOPS
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 6. The traffic stop with no traffic violation

        "Investigative" stops require no moving violation, no equipment problem —
        just officer suspicion. Hispanic investigative stops nearly doubled
        (985 → 1,868) while white investigative stops *fell*.
        """
    )
    return


@app.cell
def _(by_year, make_trend_chart, pct, stops_yr):
    _inv = by_year("number-of-stops-by-race--reason-for-stop--investigative")
    _inv_rate = pct(_inv, stops_yr)

    make_trend_chart(
        _inv_rate,
        "Investigative stops as % of all stops",
        "% of stops",
        fmt=".2f",
    )
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 7. BODY SEARCHES
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 7. Missouri police are searching fewer cars. They are searching more people.

        The share of searches involving a physical pat-down of the driver's body
        has roughly tripled for Black drivers (16% → 38%) and nearly doubled for
        Hispanic drivers (23% → 37%). Car searches fell over the same period.
        """
    )
    return


@app.cell
def _(by_year, make_trend_chart, mo, pct, searches_yr):
    _body = by_year("search-statistics--what-searched--driver")
    _body_pct = pct(_body, searches_yr)
    _car = by_year("search-statistics--what-searched--car-property")
    _car_pct = pct(_car, searches_yr)

    mo.hstack([
        make_trend_chart(
            _body_pct,
            "Body searches as % of all searches",
            "% of searches",
            width=400,
        ),
        make_trend_chart(
            _car_pct,
            "Car/property searches as % of all searches",
            "% of searches",
            width=400,
        ),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 8. WARRANT ARRESTS
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 8. Nearly six in ten Black drivers arrested at Missouri traffic stops were held on old warrants, not new crimes

        Outstanding warrants — usually for unpaid fines, missed court dates —
        account for 53–59% of all Black driver arrests at traffic stops vs ~39%
        for white drivers. Data only available for 2023–2024.
        """
    )
    return


@app.cell
def _(arrests_yr, by_year, mo, pct):
    _warrants = by_year("number-of-stops-by-race--arrest-violation--outstanding-warrent")
    _warrant_pct = pct(_warrants, arrests_yr)

    # only 2023-2024 have data, so show as a table instead of a chart
    _display = _warrant_pct[["White", "Black", "Hispanic"]].dropna()
    _display.columns = ["White %", "Black %", "Hispanic %"]

    mo.hstack([
        mo.md("### Warrant arrests as % of all arrests"),
        mo.ui.table(_display.reset_index(), selection=None),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 9. RESIST ARREST
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 9. Black drivers face resisting arrest charges at twice the rate of white drivers

        Resisting arrest is an officer-discretionary charge requiring no
        independent evidence. 5.25% of Black driver arrests in 2024 included
        it, vs 3.60% for white.
        """
    )
    return


@app.cell
def _(arrests_yr, by_year, mo, pct):
    _resist = by_year("number-of-stops-by-race--arrest-violation--resist-arrest")
    _resist_pct = pct(_resist, arrests_yr)

    _display = _resist_pct[["White", "Black", "Hispanic"]].dropna()
    _display.columns = ["White %", "Black %", "Hispanic %"]

    mo.hstack([
        mo.md("### Resisting arrest as % of all arrests"),
        mo.ui.table(_display.reset_index(), selection=None),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 10. DRUG DOG SEARCHES
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 10. Drug dogs are deployed more on white drivers. So what is triggering the searches of everyone else?

        A drug dog alert creates a paper trail — documented probable cause. But
        dogs are used in ~5% of white searches vs ~2.5% of Black searches. Black
        and Hispanic drivers are searched *more* overall, but with *less*
        documented justification.
        """
    )
    return


@app.cell
def _(by_year, make_trend_chart, pct, searches_yr):
    _dog = by_year("search-statistics--probable-cause--drug-dog-alert")
    _dog_pct = pct(_dog, searches_yr)

    make_trend_chart(
        _dog_pct,
        "Drug dog searches as % of all searches",
        "% of searches",
        fmt=".2f",
    )
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 11. SPECIAL ASSIGNMENT OFFICERS
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 11. Missouri's special assignment officers are stopping more Hispanic drivers

        "Special assignment" = drug task forces, gang units, interdiction teams.
        Hispanic stops by these officers jumped 27% in a single year (1,319 → 1,674).
        The state only started tracking officer assignment in 2023.
        """
    )
    return


@app.cell
def _(by_year, mo, pct, stops_yr):
    _spec = by_year("number-of-stops-by-race--officer-assignment--special-assignment")
    _spec_pct = pct(_spec, stops_yr)
    _display = _spec_pct[["White", "Black", "Hispanic"]].dropna()
    _display.columns = ["White %", "Black %", "Hispanic %"]

    mo.hstack([
        mo.md("### Special assignment stops as % of all stops"),
        mo.ui.table(_display.reset_index(), selection=None),
    ])
    return


# ═══════════════════════════════════════════════════════════════════════════════
# 12. CONSENT SEARCHES
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## 12. Consent searches: who is being asked

        "Do you mind if I search your car?" — from a cop on the side of the
        road. Hispanic drivers had the highest consent search share in 2020
        (44%). It has fallen, but the overall search rate has *risen*.
        """
    )
    return


@app.cell
def _(by_year, make_trend_chart, pct, searches_yr):
    _consent = by_year("search-statistics--probable-cause--consent")
    _consent_pct = pct(_consent, searches_yr)

    make_trend_chart(
        _consent_pct,
        "Consent searches as % of all searches",
        "% of searches",
    )
    return


# ═══════════════════════════════════════════════════════════════════════════════
# AGENCY DEEP DIVE — worst Hispanic arrest ratios
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ---

        ## Agency deep dive: worst Hispanic arrest rate ratios (2024)

        Filtered to agencies with at least 50 Hispanic stops so the ratios
        aren't noise. The "Ratio" column is Hispanic arrest rate ÷ white arrest
        rate. A ratio of 10.8x means a Hispanic driver at that agency was
        arrested at 10.8 times the rate of a white driver.
        """
    )
    return


@app.cell
def _(mo, pd, rows):
    _agency_stops = rows("rates-by-race--totals--all-stops")
    _agency_stops = _agency_stops[_agency_stops["year"] == 2024]

    _agency_arrests = rows("number-of-stops-by-race--stop-outcome--arrests")
    _agency_arrests = _agency_arrests[_agency_arrests["year"] == 2024]

    _merged = _agency_stops[["agency", "White", "Hispanic"]].rename(
        columns={"White": "white_stops", "Hispanic": "hisp_stops"}
    ).merge(
        _agency_arrests[["agency", "White", "Hispanic"]].rename(
            columns={"White": "white_arrests", "Hispanic": "hisp_arrests"}
        ),
        on="agency",
    )

    for _col in ["white_stops", "hisp_stops", "white_arrests", "hisp_arrests"]:
        _merged[_col] = pd.to_numeric(_merged[_col], errors="coerce")

    _merged = _merged[_merged["hisp_stops"] >= 50]
    _merged["White arrest %"] = (_merged["white_arrests"] / _merged["white_stops"] * 100).round(1)
    _merged["Hispanic arrest %"] = (_merged["hisp_arrests"] / _merged["hisp_stops"] * 100).round(1)
    _merged["Ratio"] = (_merged["Hispanic arrest %"] / _merged["White arrest %"]).round(1)
    _merged = _merged.sort_values("Ratio", ascending=False)

    _display = _merged[["agency", "hisp_stops", "Hispanic arrest %", "White arrest %", "Ratio"]].head(20)
    _display = _display.rename(columns={"agency": "Agency", "hisp_stops": "Hispanic stops"})

    mo.ui.table(_display, selection=None, page_size=20)
    return


# ═══════════════════════════════════════════════════════════════════════════════
# AGENCY COMMENTS — who speaks, who stays silent
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ## Agency comments: who responds to the state report

        The state gives every agency a chance to explain its numbers each year.
        Most of the agencies with the worst disparities have never submitted a
        single comment.
        """
    )
    return


@app.cell
def _(comments, mo, pd, rows):
    # get the top-20 worst agencies again
    _agency_stops = rows("rates-by-race--totals--all-stops")
    _agency_stops = _agency_stops[_agency_stops["year"] == 2024]
    _agency_arrests = rows("number-of-stops-by-race--stop-outcome--arrests")
    _agency_arrests = _agency_arrests[_agency_arrests["year"] == 2024]
    _merged = _agency_stops[["agency", "White", "Hispanic"]].rename(
        columns={"White": "ws", "Hispanic": "hs"}
    ).merge(
        _agency_arrests[["agency", "White", "Hispanic"]].rename(
            columns={"White": "wa", "Hispanic": "ha"}
        ),
        on="agency",
    )
    for _c in ["ws", "hs", "wa", "ha"]:
        _merged[_c] = pd.to_numeric(_merged[_c], errors="coerce")
    _merged = _merged[_merged["hs"] >= 50]
    _merged["ratio"] = ((_merged["ha"] / _merged["hs"]) / (_merged["wa"] / _merged["ws"])).round(1)
    _top = _merged.sort_values("ratio", ascending=False).head(15)["agency"].tolist()

    _rows = []
    for _agency in _top:
        _ac = comments[comments["agency"] == _agency]
        _years = _ac[_ac["has_comment"] == True]["year"].tolist()
        _rows.append({
            "Agency": _agency,
            "Years commented": ", ".join(str(y) for y in sorted(_years)) if _years else "NEVER",
            "Total comments": len(_years),
        })

    mo.ui.table(pd.DataFrame(_rows), selection=None, page_size=15)
    return


# ═══════════════════════════════════════════════════════════════════════════════
# MSHP SPOTLIGHT
# ═══════════════════════════════════════════════════════════════════════════════


@app.cell
def _(mo):
    mo.md(
        """
        ---

        ## MSHP spotlight: the largest agency, zero comments

        The Missouri State Highway Patrol makes more traffic stops than any
        other agency in the state — 312,000+ last year. They search Black
        drivers at nearly 2x the rate of white drivers. They have never
        submitted a comment to the state report.
        """
    )
    return


@app.cell
def _(STORY_COLS, alt, comments, mo, pd, rows):
    _mshp = rows("rates-by-race--totals--all-stops")
    _mshp = _mshp[_mshp["agency"] == "Missouri State Highway Patrol"]
    _mshp_yr = _mshp.groupby("year")[["Total", "White", "Black", "Hispanic"]].sum()

    _mshp_sr = rows("rates-by-race--rates--search-rate")
    _mshp_sr = _mshp_sr[_mshp_sr["agency"] == "Missouri State Highway Patrol"]

    # stops chart
    _stops_long = (
        _mshp_yr[STORY_COLS]
        .reset_index()
        .melt(id_vars="year", var_name="Race", value_name="Stops")
    )
    _stops_long["year"] = _stops_long["year"].astype(int)
    _stops_chart = (
        alt.Chart(_stops_long)
        .mark_bar()
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("Stops:Q"),
            color=alt.Color("Race:N"),
            xOffset="Race:N",
            tooltip=["Race:N", "year:O", alt.Tooltip("Stops:Q", format=",")],
        )
        .properties(width=400, height=280, title="MSHP stops by year")
    )

    # search rate table
    _sr_display = _mshp_sr[["year", "White", "Black", "Hispanic"]].copy()
    _sr_display.columns = ["Year", "White %", "Black %", "Hispanic %"]

    _mshp_comments = comments[comments["agency"] == "Missouri State Highway Patrol"]
    _n_commented = len(_mshp_comments[_mshp_comments["has_comment"] == True])

    mo.vstack([
        mo.hstack([
            _stops_chart,
            mo.vstack([
                mo.md("### MSHP search rate by year"),
                mo.ui.table(_sr_display, selection=None),
                mo.md(f"**Comments submitted:** {_n_commented} out of {len(_mshp_comments)} years"),
            ]),
        ]),
    ])
    return


if __name__ == "__main__":
    app.run()
