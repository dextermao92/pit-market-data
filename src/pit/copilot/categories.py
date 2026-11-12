"""Root-cause categories the Copilot diagnoses into. Evals score the category and the runbook, not free text.

Built from what has actually gone wrong (ops/incidents/, pipeline_runs since 2026-10-14) plus the simulated
runbooks (disk full, DuckDB lock). A diagnosis names the cause, not the symptom: 4 s TimeoutErrors are a symptom of
several of these.
"""

CATEGORIES = {
    "gateway_restarting": "IB Gateway restarting or logging in (incl. waiting for the phone 2FA): the API refuses "
                          "connections for a while, then works. Not wedged.",
    "gateway_frozen": "IB Gateway wedged: the API port accepts nothing (or IBC can't restart it) until a container "
                      "restart with a full login.",
    "secdef_stall": "IBKR's contract-definition (secdef) service stops answering, e.g. overloaded by a bulk "
                    "contract_cache refresh: requests hang, runs time out.",
    "hung_request": "One IBKR request hangs a run (no or very long request timeout, e.g. after a 1100 connectivity "
                    "loss), so the run and the slots queued behind it are late or lost.",
    "run_budget_exceeded": "Runs too slow for their budget (cold or too many chains): killed by the execution "
                           "timeout or chains skipped. The Gateway itself is fine.",
    "data_farm_disconnected": "IBKR market-data farms disconnected (2103/2105/2157 that persist): no quotes or no "
                              "spot price for the underlying.",
    "transient_reconnect": "Routine IBKR connectivity or farm blips that recovered by themselves (1100 then 1102, "
                           "2103 then 2104, daily farm drops): no data lost, no action needed.",
    "subscription_missing": "Market-data permissions: not subscribed / delayed data (354, 10089, 10090, 10167, "
                            "10168) or live data held by another session of the same user (10197).",
    "vendor_api_error": "Alpaca API errors: disconnects, 5xx, or auth failures (401/403, keys).",
    "vendor_data_gap": "Alpaca answered but bars are missing for a window (missing_bars > 0); the eod refetch "
                       "may recover them.",
    "host_down": "The host, Docker or Airflow wasn't running: slots never ran (no runs at all) or ran late in a "
                 "catch-up burst after it came back.",
    "disk_full": "Out of disk space (OSError errno 28) on the data volume.",
    "duckdb_lock": "dbt build can't take the lock on curated.duckdb because another process holds it.",
    "missing_run_records": "Monitoring artifact: runs aren't recorded in this host's pipeline_runs (e.g. data "
                           "collected on another machine), so SLIs read low although the data may exist.",
    "unknown": "The data doesn't support any cause above.",
}
