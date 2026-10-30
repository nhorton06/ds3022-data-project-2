"""Task 6: save three multi-dimensional plots to plots/ as PNG files."""

import duckdb
import matplotlib

matplotlib.use("Agg")  # no screen needed: draw straight to image files

import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402
from prefect import task  # noqa: E402

from pipeline.common import DB_PATH, PROJECT_DIR, TaskLogger  # noqa: E402
from pipeline.model import DIMENSIONS  # noqa: E402

PLOT_DIR = PROJECT_DIR / "plots"

# One fixed color per repo in every plot, assigned alphabetically so a repo's color never depends on its rank.
# (Slots 1-3 of a colorblind-safe palette, checked with a palette validator.)
REPO_COLORS = {
    "networkx/networkx": "#2a78d6",
    "numpy/numpy": "#eb6834",
    "statsmodels/statsmodels": "#1baf7a",
}
TEXT, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"

# column in mart_repo_monthly -> (panel title, y-axis label with units, log scale?)
TREND_PANELS = {
    "commits": ("Commits", "commits per month", False),
    "active_contributors": ("Active contributors", "PR authors per month", False),
    "prs_opened": ("PRs opened", "PRs per month", False),
    "merge_ratio": ("Merge ratio", "merged / (merged + closed unmerged)", False),
    "median_first_review_hours": ("Median time to first review", "hours (log scale)", True),
    "median_merge_hours": ("Median time to merge", "hours (log scale)", True),
}

plt.rcParams.update({
    "text.color": TEXT,
    "axes.labelcolor": MUTED,
    "axes.edgecolor": GRID,
    "axes.grid": True,
    "axes.axisbelow": True,  # gridlines behind bars and lines
    "grid.color": GRID,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "font.size": 10,
})

plain_number = FuncFormatter(lambda value, _: f"{value:g}")  # log-axis ticks as 1, 10, 100 instead of 10^2


def repo_legend_handles():
    return [Line2D([], [], color=color, marker="o", linewidth=2, markersize=8, label=repo)
            for repo, color in REPO_COLORS.items()]


def save(fig, name):
    path = PLOT_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def plot_dimension_profile(scores):
    """Small multiples: one panel per repo in rank order, one bar per scored dimension. Returns the PNG path."""
    dimensions = list(DIMENSIONS)
    labels = [f"{dim.replace('_', ' ').capitalize()} ({DIMENSIONS[dim][2]:.0%})" for dim in dimensions]
    # squeeze=False always returns a grid of panels, even for a single repo, so [0] is the row of panels
    fig, axes = plt.subplots(1, len(scores), figsize=(15, 4.8), sharex=True, sharey=True, squeeze=False)
    axes = axes[0]
    for ax, row in zip(axes, scores.itertuples()):
        values = [getattr(row, dim) for dim in dimensions]
        ax.barh(labels, values, color=REPO_COLORS[row.repo], height=0.6)
        for y, value in enumerate(values):
            ax.text(value + 0.02, y, f"{value:.2f}", va="center", color=MUTED, fontsize=9)
        ax.set_title(f"#{row.rank}  {row.repo}\nscore {row.score:.1f} / 100", loc="left")
        ax.set_xlim(0, 1.15)
        ax.set_xlabel("subscore (0 = least healthy of the three, 1 = healthiest)")
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", length=0)
    axes[0].invert_yaxis()  # heaviest-weighted dimension on top (shared y-axis, so this flips every panel)
    fig.suptitle("Health dimension profile, ordered by final rank (left = healthiest); weights in parentheses",
                 x=0.01, ha="left", fontsize=13)
    fig.tight_layout()
    return save(fig, "dimension_profile.png")


def plot_monthly_trends(monthly):
    """Small multiples: one panel per monthly metric, one line per repo, full window. Returns the PNG path."""
    partial_start = monthly.loc[monthly["is_partial_month"], "month"].min()
    partial_end = partial_start + pd.offsets.MonthBegin(1)
    fig, axes = plt.subplots(2, 3, figsize=(16, 8.5), sharex=True)
    for ax, (column, (title, unit, log_scale)) in zip(axes.flat, TREND_PANELS.items()):
        for repo, group in monthly.groupby("repo"):
            # small markers so a month with empty months on both sides (no line segment) still shows
            ax.plot(group["month"], group[column], color=REPO_COLORS[repo], linewidth=1.8, marker="o", markersize=2.5)
        ax.axvspan(partial_start, partial_end, color=GRID, alpha=0.8, linewidth=0)
        if log_scale:
            ax.set_yscale("log")
            ax.yaxis.set_major_formatter(plain_number)
        else:
            ax.set_ylim(bottom=0)
        ax.set_title(title, loc="left")
        ax.set_ylabel(unit)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    handles = repo_legend_handles() + [Patch(color=GRID, label="current partial month")]
    fig.legend(handles=handles, loc="upper right", ncol=4, frameon=False, bbox_to_anchor=(1, 1.0))
    fig.suptitle("Monthly activity, January 2024 to the run month (bot PRs and commits excluded)",
                 x=0.01, ha="left", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    return save(fig, "monthly_trends.png")


def plot_review_vs_merge(monthly):
    """Bubble chart, one bubble per repo per complete month: x = review time, y = merge ratio, size = contributors."""
    months = monthly[~monthly["is_partial_month"]].dropna(subset=["median_first_review_hours", "merge_ratio"])
    fig, ax = plt.subplots(figsize=(10, 7))
    for repo, group in months.groupby("repo"):
        ax.scatter(group["median_first_review_hours"], group["merge_ratio"], s=20 + 6 * group["active_contributors"],
                   color=REPO_COLORS[repo], alpha=0.7, edgecolors="white", linewidths=1.5)
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(plain_number)
    ax.set_xlabel("median time to first review that month (hours, log scale)")
    ax.set_ylabel("merge ratio that month: merged / (merged + closed unmerged)")
    ax.set_title("Review speed vs. merge ratio by month; bubble size = active contributors (one bubble = one month)",
                 loc="left")

    repo_legend = ax.legend(handles=repo_legend_handles(), title="repo", loc="lower left", frameon=False)
    ax.add_artist(repo_legend)
    size_handles = [plt.scatter([], [], s=20 + 6 * n, color=MUTED, alpha=0.4, edgecolors="white", label=f"{n}")
                    for n in (10, 40, 80)]
    ax.legend(handles=size_handles, title="active contributors", loc="lower right", frameon=False, labelspacing=1.2)
    fig.tight_layout()
    return save(fig, "review_vs_merge.png")


@task
def plot():
    """Task 6: read the scores and monthly mart, save three PNGs to plots/. Returns the saved paths."""
    log = TaskLogger("plot")
    PLOT_DIR.mkdir(exist_ok=True)
    # Not read_only: dbt keeps a connection to this file open in the same process (see validate.py)
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("SET TimeZone = 'UTC'")
        scores = con.execute("SELECT * FROM health_scores ORDER BY rank").df()
        monthly = con.execute("SELECT * FROM mart_repo_monthly ORDER BY repo, month").df()

    paths = [plot_dimension_profile(scores), plot_monthly_trends(monthly), plot_review_vs_merge(monthly)]
    for path in paths:
        log.info(f"Saved {path.relative_to(PROJECT_DIR)}")
    return [str(path) for path in paths]
