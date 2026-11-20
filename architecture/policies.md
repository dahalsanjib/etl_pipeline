# Retention, reload and recovery policy

## Retention (per layer)

| Layer | Where | Kept | Reason |
|---|---|---|---|
| Source files | `data/source/` | Never deleted by the pipeline (copied, not moved) | Original feed stays available |
| Archive | `data/archive/<yyyymmdd>/` | 1 year | Evidence and replay of the exact file loaded |
| Bronze | `STG.*` | 90 days of `FILE_DATE` partitions | Raw replay window for Silver |
| Silver | `TMP.*` | 30 days of `FILE_DATE` partitions | Rebuilt from Bronze when needed |
| Rejects | `CTL.C_REJ_TABLE` | 90 days | Time to investigate and fix the source |
| Control logs | `CTL.C_BATCH_LOG`, `C_PARAM`, `C_LOAD_DATES` | 1 year | Audit trail |
| Gold | `TGT.*` | Indefinite | Business history (SCD2 versions and facts) |

These periods are proposed values. They are documented only: no purge job is built, and nothing is deleted automatically.

## Reload policy (reprocessing a bad day)

Every step is idempotent: Bronze, Silver and `FCT_SALES` delete the date and insert it again, the dimensions and `FCT_INVENTORY` only add or close rows that changed.

| Situation | Action |
|---|---|
| Task failed | Clear the failed task in the Airflow UI. The run resumes at that task. |
| Latest loaded date is wrong (source fixed) | Rerun that date's DAG run. Safe. |
| An **earlier** date is wrong | Do **not** rerun it alone. The Gold guard blocks it, because SCD2 versions and inventory positions depend on load order. Fix the source file, truncate `TGT`, `TMP` and `CTL` tables, then run the backfill from the first date to the latest, in order. |
| Empty or missing required file | The run fails at R1 and nothing is loaded. Add the file and rerun. |

## Recovery

- **Restart point:** the failed task. `CTL.C_BATCH_LOG` shows the step, status and error text for every task.
- **Retries:** 2 automatic retries, 5 minutes apart.
- **Concurrency:** one run at a time (`max_active_runs=1`), days run in order.
- **What is committed on failure:** a failed Silver or Gold step may leave partial data for that date. Rerunning the step replaces it.
