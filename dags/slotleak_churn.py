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
    max_active_runs=6,
    tags=["repro", "slotleak", "zd93581"],
)
def slotleak_churn():
    @task(max_active_tis_per_dag=64)
    def churn(i: int) -> int:
        # Short, variable sleeps -> tasks finish rapidly and at staggered times,
        # producing frequent SIGCHLD delivery that races the reaper poll loop.
        time.sleep(random.uniform(0.5, 4.0))
        return i

    churn.expand(i=list(range(150)))


slotleak_churn()
