"""Sync-slot-leak reproduction DAG (ZD #93581 / project-everest-syncslot-leak).

Goal: churn many short tasks through a worker's sync slots fast enough to force
the agent reaper race (SIGCHLD handler vs polling loop) where a finished child
is "reaped by another signal handler" and TaskProcFinished never fires -> the
slot (and the metrics watch) leaks. The customer's leak correlated with a task
burst; this maximizes rapid child start/exit on a single 20-slot worker.

Run on: Astro Remote Execution, AstroExecutor, single default worker, syncSlots=20.
Watch worker (DEBUG) logs for: "Child process reaped by another signal handler,
ignoring" and "Task process started" pids that never get a "Task process finished".
"""

from __future__ import annotations

import random
import time
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="slotleak_churn",
    schedule="* * * * *",  # every minute -> sustained pressure with stacked runs
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=8,
    tags=["repro", "slotleak", "zd93581"],
)
def slotleak_churn():
    # v2 (intensified): FIXED short sleep so a full slot-batch (~20) exits at nearly
    # the same instant -> SIGCHLD storms that pressure the agent's SIGCHLD-handler-vs-
    # polling-loop reap race ("reaped by another signal handler" -> no TaskProcFinished
    # -> leaked slot). Staggered/random exits get reaped cleanly by the poll loop.
    @task(max_active_tis_per_dag=64)
    def churn(i: int) -> int:
        time.sleep(1.0)  # fixed -> simultaneous batch exits
        return i

    churn.expand(i=list(range(300)))


slotleak_churn()
