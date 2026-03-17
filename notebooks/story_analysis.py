"""
Story analysis: Hispanic traffic stops in Missouri are up 57 percent since 2020.
For white drivers, the increase was 10.

This script runs every analysis behind the 12 story pitches we developed from
Missouri's Vehicle Stop Report (VSR) data, 2020–2024. Each section corresponds
to a pitch and prints the key findings + saves a chart to notebooks/figures/.

Run:  uv run python notebooks/story_analysis.py
"""

import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

warnings.filterwarnings("ignore")


# ── data loading ──────────────────────────────────────────────────────────────
# The VSR pipeline writes processed data to data/out/. If you haven't run the
# pipeline locally, the script falls back to /tmp/ where the CSVs may have been
# downloaded during exploratory analysis.

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "out"

RACE_COLS = ["Total", "White", "Black", "Hispanic", "Native American", "Asian"]


def load_csv(name: str) -> pd.DataFrame:
    for directory in [DATA_DIR, Path("/tmp")]:
        path = directory / name
        if path.exists():
            return pd.read_csv(path, low_memory=False)
    raise FileNotFoundError(f"{name} not found in {DATA_DIR} or /tmp")


df = load_csv("vsr_stats.csv")
comments = load_csv("agency_comments.csv")


# ── helpers ───────────────────────────────────────────────────────────────────
# The dataset is organized by "row_key" — each row_key is a specific metric
# (e.g. total stops, search rate, contraband found) broken out by agency, year,
# and race. These helpers pull a single metric and aggregate it statewide.


def rows(key: str) -> pd.DataFrame:
    """Pull every agency's row for a given metric, with race cols as numbers."""
    sub = df[df["row_key"] == key].copy()
    for c in RACE_COLS:
        if c in sub.columns:
            sub[c] = pd.to_numeric(sub[c], errors="coerce")
    return sub


def by_year(key: str) -> pd.DataFrame:
    """Sum a metric across all agencies, grouped by year."""
    return rows(key).groupby("year")[RACE_COLS].sum()


def pct(num: pd.DataFrame, denom: pd.DataFrame) -> pd.DataFrame:
    """Express num as a percentage of denom (e.g. arrests / stops * 100)."""
    return (num / denom * 100).round(2)


# These four DataFrames are the denominators used throughout. We compute them
# once so every section can reference them without re-querying.
stops_yr = by_year("rates-by-race--totals--all-stops")
searches_yr = by_year("rates-by-race--totals--searches")
arrests_yr = by_year("number-of-stops-by-race--stop-outcome--arrests")
contraband_yr = by_year("rates-by-race--totals--contraband")


# ── chart style ───────────────────────────────────────────────────────────────
# Minimal, grayscale-ish palette. Hispanic in gold so it pops against the
# gray (white drivers) and black (Black drivers) lines.

COLORS = {"White": "#888888", "Black": "#1a1a1a", "Hispanic": "#d4a017"}
FIG_DIR = Path(__file__).resolve().parent / "figures"
FIG_DIR.mkdir(exist_ok=True)


def trend_chart(
    data: pd.DataFrame,
    cols: list[str],
    title: str,
    ylabel: str,
    fname: str,
    fmt: str = ".1f",
):
    """Line chart with labeled endpoints. Saves to figures/ directory."""
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for col in cols:
        color = COLORS.get(col, "#555555")
        ax.plot(data.index, data[col], marker="o", label=col, color=color, linewidth=2)
        # label the final data point so you can read the chart without a table
        last = data[col].iloc[-1]
        ax.annotate(
            f"{last:{fmt}}",
            (data.index[-1], last),
            textcoords="offset points",
            xytext=(8, 0),
            fontsize=9,
            color=color,
        )
    ax.set_title(title, fontsize=12, fontweight="bold", loc="left")
    ax.set_ylabel(ylabel)
    ax.legend(frameon=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(FIG_DIR / fname, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  chart → {FIG_DIR / fname}")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. THE LEAD — "Hispanic traffic stops in Missouri are up 57 percent since
#    2020. For white drivers, the increase was 10."
#
#    This is the boss's pick for the top of the story. We index every group's
#    stop count to 2020 = 100 so you can see the divergence on one chart.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("1. THE LEAD: Hispanic traffic stops up 57% since 2020")
print("=" * 72)

indexed = (stops_yr / stops_yr.iloc[0] * 100).round(1)
print("\nStops indexed to 2020 (2020 = 100):")
print(indexed[["White", "Black", "Hispanic"]])

print(f"\nRaw Hispanic stops: {stops_yr['Hispanic'].iloc[0]:,.0f} → {stops_yr['Hispanic'].iloc[-1]:,.0f}")
print(f"Raw white stops:    {stops_yr['White'].iloc[0]:,.0f} → {stops_yr['White'].iloc[-1]:,.0f}")

trend_chart(
    indexed,
    ["White", "Black", "Hispanic"],
    "Traffic stops indexed to 2020",
    "Index (2020 = 100)",
    "01_stop_surge.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. THE FLIP — "In 2020, Missouri police were less likely to search a
#    Hispanic driver than a white one. Four years later, the opposite is true."
#
#    The search rate for Hispanic drivers crossed above white drivers between
#    2020 and 2021 and has stayed above ever since. The ratio went from 0.92
#    (searched *less*) to 1.37 (searched *more*).
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("2. THE FLIP: search rate reversal")
print("=" * 72)

search_rate = pct(searches_yr, stops_yr)
search_rate["Hisp_vs_White"] = (search_rate["Hispanic"] / search_rate["White"]).round(3)
print("\nSearch rate (% of stops):")
print(search_rate[["White", "Black", "Hispanic", "Hisp_vs_White"]])

trend_chart(
    search_rate,
    ["White", "Black", "Hispanic"],
    "Search rate by race (% of stops)",
    "Search rate %",
    "02_search_rate_flip.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. STOPPED MORE, FOUND LESS — "Missouri is searching more Hispanic drivers
#    and finding less. Researchers have a name for that pattern."
#
#    The contraband "hit rate" is the classic test for pretextual enforcement:
#    if you're searching people for good reasons, you should find something.
#    Hispanic hit rate was 35% in 2020 (higher than white). By 2023: 16%.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("3. STOPPED MORE, FOUND LESS: contraband hit rate inversion")
print("=" * 72)

hit_rate = pct(contraband_yr, searches_yr)
print("\nContraband hit rate (% of searches):")
print(hit_rate[["White", "Black", "Hispanic"]])

trend_chart(
    hit_rate,
    ["White", "Black", "Hispanic"],
    "Contraband hit rate (% of searches finding contraband)",
    "Hit rate %",
    "03_contraband_hit_rate.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. THE ARREST GAP — "In some Missouri towns, Hispanic drivers are arrested
#    at traffic stops ten times as often as white drivers"
#
#    The statewide Hispanic arrest rate has climbed from 4.80% to 6.17% while
#    white arrest rate fell from 3.70% to 3.38%. Ratio: 1.30x → 1.83x.
#    At the agency level it gets extreme: Foristell 10.8x, Ballwin 8.2x.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("4. THE ARREST GAP: Hispanic arrest rate diverging")
print("=" * 72)

arrest_rate = pct(arrests_yr, stops_yr)
arrest_rate["Hisp_vs_White"] = (arrest_rate["Hispanic"] / arrest_rate["White"]).round(3)
arrest_rate["Black_vs_White"] = (arrest_rate["Black"] / arrest_rate["White"]).round(3)
print("\nArrest rate (% of stops):")
print(arrest_rate[["White", "Black", "Hispanic", "Hisp_vs_White", "Black_vs_White"]])

trend_chart(
    arrest_rate,
    ["White", "Black", "Hispanic"],
    "Arrest rate by race (% of stops)",
    "Arrest rate %",
    "04_arrest_rate_gap.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. EQUIPMENT VIOLATIONS — "Missouri officers stopped nearly 6,000 hispanic
#    drivers for equipment violations in 2024 but for white drivers, the
#    number barely changed."
#
#    Equipment violations (broken taillight, tinted windows, etc.) are the most
#    officer-discretionary reason for a stop. Hispanic equipment stops are up
#    75% since 2020. White: up 6%. This is the pretext door.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("5. EQUIPMENT VIOLATIONS: the pretext door")
print("=" * 72)

# note: the row_key has a typo in the source data ("equpiment")
equip = by_year("number-of-stops-by-race--reason-for-stop--equpiment")
equip_idx = (equip / equip.iloc[0] * 100).round(1)
print("\nEquipment stops indexed to 2020:")
print(equip_idx[["White", "Black", "Hispanic"]])
print(f"\nHispanic raw: {equip['Hispanic'].iloc[0]:,.0f} → {equip['Hispanic'].iloc[-1]:,.0f} (+{(equip['Hispanic'].iloc[-1]/equip['Hispanic'].iloc[0]-1)*100:.0f}%)")

trend_chart(
    equip_idx,
    ["White", "Black", "Hispanic"],
    "Equipment violation stops indexed to 2020",
    "Index (2020 = 100)",
    "05_equipment_stops.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. INVESTIGATIVE STOPS — "The traffic stop with no traffic violation"
#
#    "Investigative" is the most subjective stop reason on the form — no moving
#    violation, no equipment problem, just officer suspicion. Hispanic
#    investigative stops nearly doubled (985 → 1,868) while white fell.
#    Hispanic rate now exceeds white for the first time.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("6. INVESTIGATIVE STOPS: no violation required")
print("=" * 72)

inv = by_year("number-of-stops-by-race--reason-for-stop--investigative")
inv_rate = pct(inv, stops_yr)
print("\nInvestigative stops as % of all stops:")
print(inv_rate[["White", "Black", "Hispanic"]])
print(f"\nHispanic raw: {inv['Hispanic'].iloc[0]:,.0f} → {inv['Hispanic'].iloc[-1]:,.0f} (+{(inv['Hispanic'].iloc[-1]/inv['Hispanic'].iloc[0]-1)*100:.0f}%)")

trend_chart(
    inv_rate,
    ["White", "Black", "Hispanic"],
    "Investigative stops as % of all stops",
    "% of stops",
    "06_investigative_stops.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. BODY SEARCHES — "Missouri police are searching fewer cars. They are
#    searching more people."
#
#    The share of searches that involve physically searching the driver's body
#    has roughly tripled for Black drivers (16% → 38%) and nearly doubled for
#    Hispanic drivers (23% → 37%). Meanwhile car/property searches are falling.
#    Nobody announced this shift. It just shows up in the data.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("7. BODY SEARCHES: searching fewer cars, more people")
print("=" * 72)

body = by_year("search-statistics--what-searched--driver")
body_pct = pct(body, searches_yr)
car = by_year("search-statistics--what-searched--car-property")
car_pct = pct(car, searches_yr)

print("\nBody search as % of all searches:")
print(body_pct[["White", "Black", "Hispanic"]])
print("\nCar/property search as % of all searches:")
print(car_pct[["White", "Black", "Hispanic"]])

trend_chart(
    body_pct,
    ["White", "Black", "Hispanic"],
    "Body searches as % of all searches",
    "% of searches",
    "07_body_searches.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. WARRANT ARRESTS — "Nearly six in ten Black drivers arrested at Missouri
#    traffic stops were held on old warrants, not new crimes"
#
#    This data only exists for 2023–2024. 59% of Black driver arrests in 2023
#    were for outstanding warrants (unpaid fines, missed court dates) — vs 38%
#    for white drivers. The traffic stop doesn't create the warrant. It just
#    closes the trap.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("8. WARRANT ARRESTS: old debts, new handcuffs")
print("=" * 72)

warrants = by_year("number-of-stops-by-race--arrest-violation--outstanding-warrent")
warrant_pct = pct(warrants, arrests_yr)
print("\nWarrant arrests as % of all arrests:")
print(warrant_pct[["White", "Black", "Hispanic"]])
print(f"\nBlack warrant arrests 2024: {warrants.loc[2024, 'Black']:,.0f}")
print(f"White warrant arrests 2024: {warrants.loc[2024, 'White']:,.0f}")

if len(warrant_pct) >= 2:
    trend_chart(
        warrant_pct,
        ["White", "Black", "Hispanic"],
        "Warrant arrests as % of all arrests",
        "% of arrests",
        "08_warrant_arrests.png",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 9. RESIST ARREST — "Black drivers in Missouri face resisting arrest charges
#    at twice the rate of white drivers. There are no independent witnesses."
#
#    Resisting arrest is an officer-only charge — no body cam, no bystander,
#    just the officer's account. 5.25% of Black driver arrests include it,
#    vs 3.60% for white. It forecloses complaint.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("9. RESIST ARREST: the charge with no witnesses")
print("=" * 72)

resist = by_year("number-of-stops-by-race--arrest-violation--resist-arrest")
resist_pct = pct(resist, arrests_yr)
print("\nResist arrest as % of all arrests:")
print(resist_pct[["White", "Black", "Hispanic"]])

if len(resist_pct) >= 2:
    trend_chart(
        resist_pct,
        ["White", "Black", "Hispanic"],
        "Resisting arrest charges as % of all arrests",
        "% of arrests",
        "09_resist_arrest.png",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 10. DRUG DOG SEARCHES — "Drug dogs are deployed more on white drivers. So
#     what is triggering the searches of everyone else?"
#
#     Drug dogs provide a paper trail — the dog alerted, so there's probable
#     cause. But they're used disproportionately on white drivers (5% of white
#     searches vs 2.5% of Black searches). Black and Hispanic drivers are
#     searched at higher rates overall, but with LESS documented justification.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("10. DRUG DOG SEARCHES: probable cause on a leash")
print("=" * 72)

dog = by_year("search-statistics--probable-cause--drug-dog-alert")
dog_pct = pct(dog, searches_yr)
print("\nDrug dog as % of all searches:")
print(dog_pct[["White", "Black", "Hispanic"]])

search_rate_display = pct(searches_yr, stops_yr)
print("\nFor context — overall search rate:")
print(search_rate_display[["White", "Black", "Hispanic"]])

trend_chart(
    dog_pct,
    ["White", "Black", "Hispanic"],
    "Drug dog searches as % of all searches",
    "% of searches",
    "10_drug_dog.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# 11. SPECIAL ASSIGNMENT — "Missouri's special assignment officers are stopping
#     more Hispanic drivers. The state only started tracking it two years ago."
#
#     "Special assignment" = drug task forces, gang units, interdiction teams.
#     These aren't traffic cops. Hispanic stops by these officers jumped 27%
#     in a single year. Only tracked since 2023 so there's no long baseline.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("11. SPECIAL ASSIGNMENT OFFICERS")
print("=" * 72)

spec = by_year("number-of-stops-by-race--officer-assignment--special-assignment")
spec_pct = pct(spec, stops_yr)
print("\nSpecial assignment stops as % of all stops:")
print(spec_pct[["White", "Black", "Hispanic"]])
print(f"\nHispanic stops by special assignment: {spec.loc[2023, 'Hispanic']:,.0f} (2023) → {spec.loc[2024, 'Hispanic']:,.0f} (2024)")


# ═══════════════════════════════════════════════════════════════════════════════
# 12. CONSENT SEARCHES — bonus analysis
#
#     Consent searches are the most legally fraught type — "do you mind if I
#     search your car?" from a cop on the side of the road. Hispanic drivers
#     had the highest consent search share in 2020 (44% of all their searches).
#     It has fallen since, but the overall search rate has risen. The question
#     for a lawyer: are officers finding other justifications now?
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("12. CONSENT SEARCHES: who is being asked")
print("=" * 72)

consent = by_year("search-statistics--probable-cause--consent")
consent_pct = pct(consent, searches_yr)
print("\nConsent as % of all searches:")
print(consent_pct[["White", "Black", "Hispanic"]])

trend_chart(
    consent_pct,
    ["White", "Black", "Hispanic"],
    "Consent searches as % of all searches",
    "% of searches",
    "11_consent_searches.png",
)


# ═══════════════════════════════════════════════════════════════════════════════
# AGENCY DEEP DIVE — which departments have the worst Hispanic arrest ratios?
#
# We filter to agencies with at least 50 Hispanic stops in 2024 so the ratios
# aren't noise from tiny sample sizes. Then we check the comments database to
# see which of these agencies have ever bothered to explain themselves.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("AGENCY DEEP DIVE: worst Hispanic arrest rate ratios (2024)")
print("=" * 72)

agency_stops = rows("rates-by-race--totals--all-stops")
agency_stops = agency_stops[agency_stops["year"] == 2024]

agency_arrests = rows("number-of-stops-by-race--stop-outcome--arrests")
agency_arrests = agency_arrests[agency_arrests["year"] == 2024]

# merge stops and arrests for each agency so we can compute the ratio
merged = agency_stops[["agency", "White", "Hispanic"]].rename(
    columns={"White": "white_stops", "Hispanic": "hisp_stops"}
).merge(
    agency_arrests[["agency", "White", "Hispanic"]].rename(
        columns={"White": "white_arrests", "Hispanic": "hisp_arrests"}
    ),
    on="agency",
)

for col in ["white_stops", "hisp_stops", "white_arrests", "hisp_arrests"]:
    merged[col] = pd.to_numeric(merged[col], errors="coerce")

merged = merged[merged["hisp_stops"] >= 50]  # throw out tiny sample sizes
merged["white_arrest_rate"] = merged["white_arrests"] / merged["white_stops"]
merged["hisp_arrest_rate"] = merged["hisp_arrests"] / merged["hisp_stops"]
merged["ratio"] = (merged["hisp_arrest_rate"] / merged["white_arrest_rate"]).round(2)
merged = merged.sort_values("ratio", ascending=False)

print(f"\nAgencies with ≥50 Hispanic stops in 2024 (n={len(merged)}):")
print(f"\n{'Agency':<40} {'H stops':>7} {'H arr%':>7} {'W arr%':>7} {'Ratio':>6}")
print("-" * 72)
for _, r in merged.head(20).iterrows():
    print(
        f"{r['agency']:<40} {r['hisp_stops']:>7.0f} "
        f"{r['hisp_arrest_rate']*100:>6.1f}% {r['white_arrest_rate']*100:>6.1f}% "
        f"{r['ratio']:>6.1f}x"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# AGENCY COMMENTS — the silence is the story
#
# The state gives every agency a chance to explain its numbers. Most of the
# worst offenders have never submitted a single comment. We check who has and
# who hasn't, because that's the accountability spine of the story.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("AGENCY COMMENTS: who responds to the state report")
print("=" * 72)

top_agencies = merged.head(15)["agency"].tolist()
for agency in top_agencies:
    agency_comments = comments[comments["agency"] == agency]
    years_commented = agency_comments[agency_comments["has_comment"] == True]["year"].tolist()
    if years_commented:
        print(f"  {agency}: commented in {years_commented}")
    else:
        print(f"  {agency}: NEVER COMMENTED")


# ═══════════════════════════════════════════════════════════════════════════════
# MSHP SPOTLIGHT — Missouri State Highway Patrol
#
# The single largest agency in the dataset. 312,000+ stops last year. They
# search Black drivers at nearly 2x the rate of white drivers, Hispanic at
# nearly 2x. They have never submitted a comment. Not once in five years.
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print("MSHP SPOTLIGHT: the largest agency, zero comments")
print("=" * 72)

mshp = rows("rates-by-race--totals--all-stops")
mshp = mshp[mshp["agency"] == "Missouri State Highway Patrol"]
mshp_yr = mshp.groupby("year")[["Total", "White", "Black", "Hispanic"]].sum()
print("\nMSHP stops by year:")
print(mshp_yr)

mshp_sr = rows("rates-by-race--rates--search-rate")
mshp_sr = mshp_sr[mshp_sr["agency"] == "Missouri State Highway Patrol"]
print("\nMSHP search rate:")
print(mshp_sr[["year", "White", "Black", "Hispanic"]].to_string(index=False))

mshp_comments = comments[comments["agency"] == "Missouri State Highway Patrol"]
commented = mshp_comments[mshp_comments["has_comment"] == True]
print(f"\nMSHP comments submitted: {len(commented)} out of {len(mshp_comments)} years")


# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 72)
print(f"Done. {len(list(FIG_DIR.glob('*.png')))} charts saved to {FIG_DIR}/")
print("=" * 72)
