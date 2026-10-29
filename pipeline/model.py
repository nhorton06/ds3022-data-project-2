"""Task 5: score and rank the repos on six health dimensions read from mart_health_inputs."""

import duckdb
import pandas as pd
from prefect import task

from pipeline.common import DB_PATH, TaskLogger

# dimension -> (metric in mart_health_inputs, higher is healthier?, weight)
# Weights follow the order of ASSESSING-HEALTH.md: maintainer responsiveness #1, bus factor #2, recency of
# activity #3, release practices #4, PR backlog #5, PR acceptance / time to merge #6. Raw volume (#13) isn't scored.
DIMENSIONS = {
    "responsiveness": ("pct_reviewed_within_7d", True, 0.25),
    "contributor_base": ("bus_factor", True, 0.20),
    "activity_trend": ("commit_trend_pct", True, 0.15),
    "release_cadence": ("releases_per_year", True, 0.15),
    "backlog": ("pct_open_stale", False, 0.15),
    "throughput": ("median_hours_to_merge", False, 0.10),
}


def min_max(values, higher_is_healthier):
    """Scale a metric to 0-1 across the repos so 1 = healthiest and 0 = least healthy. A full tie scores 0.5."""
    low, high = values.min(), values.max()
    if high == low:
        return values * 0 + 0.5
    scaled = (values - low) / (high - low)
    return scaled if higher_is_healthier else 1 - scaled


@task
def model_health():
    """Task 5: min-max each metric, weight the six dimension subscores into a 0-100 score, rank, and save."""
    log = TaskLogger("model")
    assert abs(sum(weight for _, _, weight in DIMENSIONS.values()) - 1) < 1e-9, "weights must sum to 1"

    # Not read_only: dbt keeps a connection to this file open in the same process (see validate.py), and this task writes
    with duckdb.connect(str(DB_PATH)) as con:
        inputs = con.execute("SELECT * FROM mart_health_inputs").df().set_index("repo")

        scores = pd.DataFrame(index=inputs.index)
        for dimension, (metric, higher_is_healthier, _) in DIMENSIONS.items():
            scores[dimension] = min_max(inputs[metric], higher_is_healthier)
        scores["score"] = 100 * sum(scores[dim] * weight for dim, (_, _, weight) in DIMENSIONS.items())
        scores["rank"] = scores["score"].rank(ascending=False, method="min").astype(int)
        ranked = scores.reset_index().sort_values("rank")[["rank", "repo", "score", *DIMENSIONS]]

        con.register("ranked_df", ranked)
        con.execute("CREATE OR REPLACE TABLE health_scores AS SELECT * FROM ranked_df")

    log.info(f"Inputs from mart_health_inputs\n{inputs.round(4).to_string()}")
    log.info(
        "Health ranking (score 0-100; dimension subscores 0-1, 1 = healthiest of the three), "
        f"written to health_scores\n{ranked.round(3).to_string(index=False)}"
    )
    return ranked.to_dict("records")
