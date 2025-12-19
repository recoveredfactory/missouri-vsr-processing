from __future__ import annotations

import argparse
import csv
import logging
import re
import unicodedata
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
from rapidfuzz import process, fuzz


def _normalize_name(text: str) -> str:
    """Normalize agency names for comparison."""
    if text is None:
        return ""
    # Basic ASCII-friendly normalization with a light synonym for "&".
    t = unicodedata.normalize("NFKC", str(text))
    t = t.replace("’", "'").replace("&", " and ")
    t = re.sub(r"[^a-z0-9]+", " ", t.lower())
    return " ".join(t.split())


def _repo_root(start: Path | None = None) -> Path:
    """Return repository root by walking up until a pyproject.toml (or .git) is found."""
    cur = (start or Path.cwd()).resolve()
    for p in [cur, *cur.parents]:
        if (p / "pyproject.toml").exists() or (p / ".git").exists():
            return p
    return cur


def _resolve_maybe_repo(path: Path) -> Path:
    """Resolve a path; if it doesn't exist, also try repo_root / path."""
    if path.exists():
        return path
    alt = _repo_root() / path
    return alt


def _load_agency_source(source_parquet: Path | None, source_excel: Path | None, log: logging.Logger) -> pd.DataFrame:
    # Prefer Excel if provided and exists (explicit source), otherwise fall back to Parquet
    if source_excel:
        se = _resolve_maybe_repo(source_excel)
        if se.exists():
            log.info("Loading agency list from Excel: %s", se)
            return pd.read_excel(se, engine="openpyxl")
    if source_parquet:
        sp = _resolve_maybe_repo(source_parquet)
        if sp.exists():
            log.info("Loading agency list from Parquet: %s", sp)
            return pd.read_parquet(sp)
    raise FileNotFoundError(
        f"Provide --source-parquet or --source-excel (not found: {source_parquet} or {source_excel})"
    )


def _load_crosswalk(csv_path: Path, log: logging.Logger) -> pd.DataFrame:
    p = _resolve_maybe_repo(csv_path)
    if not p.exists():
        log.info("Starting new crosswalk: %s", p)
        return pd.DataFrame(columns=["Normalized", "Raw", "Canonical"])
    log.info("Loading existing crosswalk: %s", p)
    return pd.read_csv(p)


def _candidate_pool(vsr_df: pd.DataFrame, crosswalk: pd.DataFrame) -> List[str]:
    # Candidates are all distinct departments from the combined VSR output
    if "Department" not in vsr_df.columns:
        raise ValueError("VSR parquet missing 'Department' column")
    raw_names = sorted(set(str(v) for v in vsr_df["Department"].dropna().unique()))
    canon = sorted(set(str(v) for v in crosswalk.get("Canonical", pd.Series(dtype=object)).dropna().unique()))
    # Prioritize Canonical first, then fall back to raw names
    return canon + [n for n in raw_names if n not in canon]


def _suggest(candidates: List[str], query: str, limit: int = 5) -> List[Tuple[str, int]]:
    """Suggest best candidate Departments for a given agency name.

    Scores are computed on normalized strings using a blend of RapidFuzz scorers
    with a small prefix bonus. This tends to favor very similar names and stable
    initial characters while remaining robust to reordering.
    """
    norm_q = _normalize_name(query)
    scored: List[Tuple[str, int]] = []
    for cand in candidates:
        norm_c = _normalize_name(cand)
        # Base scorers
        s_sort = fuzz.token_sort_ratio(norm_q, norm_c)
        s_ratio = fuzz.ratio(norm_q, norm_c)
        s_part = fuzz.partial_ratio(norm_q, norm_c)
        s_set = fuzz.token_set_ratio(norm_q, norm_c)
        # Prefix bonus: reward common prefix matches (capped)
        bonus = 0
        if norm_q and norm_c:
            if norm_c.startswith(norm_q) or norm_q.startswith(norm_c):
                # Strong prefix match
                bonus = 8
            else:
                # smaller bonus for shared starting chars
                common = 0
                for a, b in zip(norm_q, norm_c):
                    if a == b:
                        common += 1
                    else:
                        break
                if common >= 3:
                    bonus = min(6, common)  # up to 6
        # Weighted blend
        score = int(round(0.6 * s_sort + 0.25 * s_ratio + 0.15 * s_part + bonus))
        scored.append((cand, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:limit]


def _write_crosswalk(df: pd.DataFrame, path: Path, log: logging.Logger) -> None:
    p = _resolve_maybe_repo(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, quoting=csv.QUOTE_MINIMAL)
    log.info("Wrote crosswalk → %s (%d rows)", p, len(df))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Interactive agency crosswalk builder")
    p.add_argument("--source-parquet", type=Path, default=Path("data/processed/agency_list.parquet"), help="Path to agency list Parquet")
    p.add_argument("--source-excel", type=Path, default=Path("data/src/2025-05-05-post-law-enforcement-agencies-list.xlsx"), help="Path to agency list Excel (fallback)")
    p.add_argument("--vsr-parquet", type=Path, default=Path("data/processed/all_combined_output.parquet"), help="Path to combined VSR Parquet (for Department candidates)")
    p.add_argument("--crosswalk", type=Path, default=Path("data/src/agency_crosswalk.csv"), help="Path to crosswalk CSV to create/update")
    p.add_argument("--name-col", type=str, default=None, help="Column name in agency list to treat as the agency/department name")
    p.add_argument("--top-k", type=int, default=5, help="Number of suggestions to display")
    p.add_argument("--merge-output", type=Path, default=Path("data/processed/agency_reference.parquet"), help="Optional path to write merged reference parquet (agency + crosswalk)")
    p.add_argument("--verbose", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    log = logging.getLogger("vsr_crosswalk")

    # Load sources (resolve relative to repo root if needed)
    try:
        agency_df = _load_agency_source(args.source_parquet, args.source_excel, log)
    except FileNotFoundError as e:
        log.error(str(e))
        return 2
    crosswalk_df = _load_crosswalk(args.crosswalk, log)
    vsr_path = _resolve_maybe_repo(args.vsr_parquet)
    if not vsr_path.exists():
        log.error("VSR Parquet not found: %s", vsr_path)
        return 2
    vsr_df = pd.read_parquet(vsr_path)

    # Pick a name column
    name_col = args.name_col
    if not name_col:
        name_col = next((c for c in agency_df.columns if str(c).lower() in {"department", "agency", "name"}), None)
        if not name_col:
            name_col = agency_df.columns[0]
    log.info("Using name column: %s", name_col)

    # Build normalized values from agency list
    work = agency_df[[name_col]].rename(columns={name_col: "Raw"}).copy()
    work["Normalized"] = work["Raw"].astype(str).apply(_normalize_name)
    work = work.drop_duplicates("Normalized")

    # Merge in existing crosswalk mappings
    if not crosswalk_df.empty:
        crosswalk_df["Normalized"] = crosswalk_df["Normalized"].astype(str)
        work = work.merge(crosswalk_df, on="Normalized", how="left", suffixes=("", "_xw"))
        # Keep Canonical if already present
        if "Canonical_xw" in work.columns and "Canonical" in work.columns:
            work["Canonical"] = work["Canonical"].fillna(work["Canonical_xw"])
        # Preserve Raw from existing file if present
        if "Raw_xw" in work.columns:
            work["Raw"] = work["Raw_xw"].fillna(work["Raw"])
        # Drop helper cols
        work = work[["Normalized", "Raw", "Canonical"]]
    # Ensure Canonical column exists
    if "Canonical" not in work.columns:
        work["Canonical"] = pd.NA

    # Interactive filling
    candidates = _candidate_pool(vsr_df, crosswalk_df)
    pending = work[work["Canonical"].isna()].copy()
    # Auto-accept exact normalized matches against candidate departments
    norm_map = { _normalize_name(c): c for c in candidates }
    auto_mask = pending["Normalized"].map(norm_map.__contains__)
    auto_rows = []
    if auto_mask.any():
        for _, r in pending[auto_mask].iterrows():
            canonical = norm_map.get(r["Normalized"]) or r["Raw"]
            auto_rows.append({"Normalized": r["Normalized"], "Raw": r["Raw"], "Canonical": canonical})
        pending = pending[~auto_mask].copy()
        # Log a brief sample of auto-matched pairs for clarity
        sample_ct = min(5, len(auto_rows))
        sample = [
            {"raw": ar["Raw"], "canonical": ar["Canonical"]}
            for ar in auto_rows[:sample_ct]
        ]
        log.info(
            "Auto-matched %d rows on exact normalized names. Sample: %s",
            len(auto_rows),
            sample,
        )

    out_rows = []
    # Preserve completed rows
    out_rows.extend(work[work["Canonical"].notna()].to_dict(orient="records"))
    # Add auto-accepted rows
    if auto_rows:
        out_rows.extend(auto_rows)
    # Progress summary after removing exact matches
    total = len(work)
    matched = len(out_rows)
    mapped = sum(1 for r in out_rows if (r.get("Canonical") not in (None, "")))
    skipped_ct = sum(1 for r in out_rows if (r.get("Canonical") == ""))
    remaining = len(pending)
    log.info("Progress: %d/%d resolved (mapped: %d, skipped: %d, remaining: %d)", matched, total, mapped, skipped_ct, remaining)
    # Interactive fill for remaining, with back/More support and autosave after each choice
    pend_df = pending.sort_values("Normalized").reset_index(drop=True)
    decided_stack: List[int] = []  # indices we've decided in this session
    # Resume support: start from last processed normalized if state exists
    state_path = _resolve_maybe_repo(args.crosswalk).with_suffix(".state.json")
    k = 0
    if state_path.exists():
        try:
            st = json.loads(state_path.read_text())
            last_norm = st.get("last_normalized")
            if last_norm:
                # advance to next after last_norm
                idxs = pend_df.index[pend_df["Normalized"] == last_norm].tolist()
                if idxs:
                    k = idxs[0] + 1
                else:
                    # first index greater than last_norm
                    greater = pend_df[pend_df["Normalized"] > last_norm]
                    if not greater.empty:
                        k = int(greater.index.min())
        except Exception:
            pass
    while k < len(pend_df):
        r = pend_df.iloc[k]
        raw = r["Raw"]
        norm = r["Normalized"]
        # Suggest loop with pagination (m = more). Show header on each page.
        page = 0
        while True:
            sugg_all = _suggest(candidates, raw, limit=1000)
            start = page * args.top_k
            end = start + args.top_k
            to_show = sugg_all[start:end]
            log.info("\n[%d/%d] Raw (agency list, column %s): %s\nNormalized: %s", k + 1, len(pend_df), name_col, raw, norm)
            if not to_show:
                print("  [no more suggestions]")
                print("  b. back (previous)")
                print("  s. skip (leave blank)")
                print("  q. save & quit")
                choice = input("Select b / s / q: ").strip().lower()
                if choice == "q":
                    out = pd.DataFrame(out_rows)
                    _write_crosswalk(out.sort_values("Normalized"), args.crosswalk, log)
                    try:
                        state_path = _resolve_maybe_repo(args.crosswalk).with_suffix(".state.json")
                        state_path.write_text(json.dumps({"last_normalized": norm}))
                    except Exception:
                        pass
                    return 0
                if choice == "b":
                    if out_rows:
                        out_rows.pop()
                        k = max(0, k-1)
                        break
                    else:
                        continue
                if choice == "s" or choice == "":
                    # Skip means "mark as done"; record empty Canonical to avoid re-prompting next run
                    canonical = ""
                    out_rows.append({"Normalized": norm, "Raw": raw, "Canonical": canonical})
                    _write_crosswalk(pd.DataFrame(out_rows).sort_values("Normalized"), args.crosswalk, log)
                    try:
                        state_path = _resolve_maybe_repo(args.crosswalk).with_suffix(".state.json")
                        state_path.write_text(json.dumps({"last_normalized": norm}))
                    except Exception:
                        pass
                    k += 1
                    break
                continue
            for i, (s, score) in enumerate(to_show, start=1):
                print(f"  {i}. {s} ({score}) [norm: {_normalize_name(s)}]")
            print("  m. more suggestions")
            print("  b. back (previous)")
            print("  s. skip (leave blank)")
            print("  q. save & quit")
            choice = input(f"Select [1-{len(to_show)}] / m / b / s / q: ").strip().lower()
            if choice == "q":
                # Save progress (resolved only) and write resume state
                out = pd.DataFrame(out_rows)
                _write_crosswalk(out.sort_values("Normalized"), args.crosswalk, log)
                try:
                    state_path.write_text(json.dumps({"last_normalized": norm}))
                except Exception:
                    pass
                return 0
            if choice == "m":
                page += 1
                continue
            if choice == "b":
                if decided_stack:
                    # Remove last decision and step back
                    last_idx = decided_stack.pop()
                    # Remove last appended from out_rows
                    if out_rows:
                        out_rows.pop()
                    # Move k to previous index
                    k = max(0, last_idx)
                    # Save rollback and redisplay previous row
                    _write_crosswalk(pd.DataFrame(out_rows).sort_values("Normalized"), args.crosswalk, log)
                    break
                else:
                    print("No previous decision to go back to.")
                    continue
            if choice == "s" or choice == "":
                # Skip → record empty Canonical so this row is considered resolved
                canonical = ""
            else:
                try:
                    idx = int(choice)
                    if 1 <= idx <= len(to_show):
                        canonical = to_show[idx - 1][0]
                    else:
                        print("Invalid selection")
                        continue
                except ValueError:
                    print("Invalid selection")
                    continue
            # Record decision
            out_rows.append({"Normalized": norm, "Raw": raw, "Canonical": canonical})
            decided_stack.append(k)
            # Autosave resolved only and update resume state
            _write_crosswalk(pd.DataFrame(out_rows).sort_values("Normalized"), args.crosswalk, log)
            try:
                state_path.write_text(json.dumps({"last_normalized": norm}))
            except Exception:
                pass
            k += 1
            break

    out = pd.DataFrame(out_rows)
    _write_crosswalk(out.sort_values("Normalized"), args.crosswalk, log)
    # Final progress summary (mapped vs skipped)
    matched_final = len(out_rows)
    mapped_final = sum(1 for r in out_rows if (r.get("Canonical") not in (None, "")))
    skipped_final = sum(1 for r in out_rows if (r.get("Canonical") == ""))
    total_final = len(work)
    log.info("Done. Progress: %d/%d resolved (mapped: %d, skipped: %d)", matched_final, total_final, mapped_final, skipped_final)

    # Optionally write merged agency_reference for convenience
    if args.merge_output:
        merged = agency_df.copy()
        norm_col = args.name_col or next((c for c in merged.columns if str(c).lower() in {"department", "agency", "name"}), merged.columns[0])
        merged["Normalized"] = merged[norm_col].astype(str).apply(_normalize_name)
        cw = pd.read_csv(_resolve_maybe_repo(args.crosswalk))
        merged = merged.merge(cw, on="Normalized", how="left", suffixes=("", "_cw"))
        outp = _resolve_maybe_repo(args.merge_output)
        outp.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(outp, index=False, engine="pyarrow")
        log.info("Wrote merged reference Parquet → %s (%d rows)", outp, len(merged))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
