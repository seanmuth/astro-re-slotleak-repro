"""Sync-slot-leak reproduction DAG v3 (ZD #93581 / project-everest-syncslot-leak).

v1/v2 (slotleak_churn.py) tried to trip the leak with clean sleep-churn alone.
That did NOT reproduce: clean single-process exits get reaped cleanly by the
agent's 0.1s polling waitpid(WNOHANG) loop (0 signal-kills, 0 pid-reuse, 0
orphans in a live soak). v3 stops relying on volume alone and instead stacks the
two failure modes that ARE locally reproducible, per the ranked mechanism
analysis:

  Rank 2 - Partial kills: task child ignores SIGTERM, so a short
           execution_timeout forces the task-SDK supervisor to escalate
           SIGTERM -> SIGKILL against a signal-ignoring process. This exercises
           the timeout-escalation path (supervisor.py:~1010-1067) and the
           reaper's handling of a child that only dies on SIGKILL, widening the
           window in which the agent can lose the completion notification.

  Rank 3 - Multi-process subtree + metrics-collector race: task spawns
           DISOWNED grandchildren (setsid / nohup, backgrounded) that OUTLIVE
           the tracked supervisor pid. Because the task-runner grandchild is a
           bare os.fork with NO setsid/subreaper (supervisor.py:712), these
           descendants survive the tracked pid's exit. The metrics collector
           recurses task children (collector.py:174-240) and can psutil-touch /
           reap descendants racing the reaper's waitpid -> ECHILD -> missed
           TaskProcFinished -> leaked slot + leaked metrics watch (matches the
           351 dead-but-still-watched PIDs in the customer worker logs).

  High fork churn: mapped fan-out + every-minute schedule + high max_active so
           many supervisor fork/exit cycles overlap, amplifying both races. This
           is the "under load" condition (Rank 5 event-loop stall widens the
           reap window) that an idle rig lacks.

Run on: Astro Remote Execution, AstroExecutor, single default worker
(3Gi / 2cpu), syncSlots=20. Watch worker (DEBUG) logs for:
  - "reaped by another signal handler" (coordinator/worker.py:239 relic branch)
  - "Task process started" pids that never get a matching "Task process finished"
  - rising "Error collecting metrics for process" (dead-PID watch never unwatched)
  - sync_slots_available staircase toward 0 without a pod restart.

Does NOT touch slotleak_churn.py; runs alongside it as an independent DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


# ---------------------------------------------------------------------------
# Rank 2 - Partial kill: the child installs `trap '' TERM INT` so it IGNORES
# SIGTERM/SIGINT, then sleeps far longer than execution_timeout. When the
# supervisor hits the ~25s timeout it sends SIGTERM (ignored), waits, then
# escalates to SIGKILL. This is the SIGTERM->SIGKILL escalation against a
# signal-ignoring process that the clean churn DAG never triggered.
# ---------------------------------------------------------------------------
RANK2_PARTIAL_KILL_CMD = r"""
echo "[rank2] pid=$$ installing TERM/INT trap (ignore), then sleeping 3600s";
trap '' TERM INT;
sleep 3600
"""

# ---------------------------------------------------------------------------
# Rank 3 - Multi-process subtree: spawn DISOWNED grandchildren that OUTLIVE the
# task. `setsid` detaches into its own session/process group; `nohup ... &
# disown` backgrounds and removes the job from the shell so nothing waits on it.
# The task itself exits 0 immediately, so the tracked supervisor pid dies while
# these descendants are still alive -> the reaper + metrics collector must deal
# with descendants after the tracked pid is gone (ECHILD / NoSuchProcess race).
# ---------------------------------------------------------------------------
RANK3_ORPHAN_SUBTREE_CMD = r"""
echo "[rank3] pid=$$ spawning disowned grandchildren that outlive the task";
setsid sleep 300 &
disown 2>/dev/null || true;
nohup sleep 120 >/dev/null 2>&1 &
disown 2>/dev/null || true;
echo "[rank3] grandchildren detached; task exiting 0 while they live on";
exit 0
"""

FANOUT = 120  # high fork churn on a 20-slot worker; runnable on 3Gi/2cpu


@dag(
    dag_id="slotleak_v3",
    schedule="* * * * *",  # every minute -> stacked runs, sustained fork pressure
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=8,  # overlap runs to keep the reap window under load
    tags=["repro", "slotleak", "zd93581", "v3", "rank2", "rank3"],
)
def slotleak_v3():
    # -- Rank 2: fan-out of SIGTERM-ignoring tasks with a tight execution_timeout.
    # Each one forces a SIGTERM->SIGKILL escalation ~25s in. Mapped so a batch of
    # escalations lands together, pressuring the reaper under load.
    partial_kill = BashOperator.partial(
        task_id="rank2_partial_kill",
        bash_command=RANK2_PARTIAL_KILL_CMD,
        execution_timeout=timedelta(seconds=25),  # short -> forces escalation
        retries=0,  # don't mask; we want the kill path every run
        # cap concurrent kills so the worker (20 slots) stays runnable
        max_active_tis_per_dag=20,
    ).expand(env=[{} for _ in range(FANOUT)])

    # -- Rank 3: fan-out of tasks that leave disowned grandchildren behind.
    # These exit fast (high churn) but seed descendants for the metrics-collector
    # / reaper race.
    orphan_subtree = BashOperator.partial(
        task_id="rank3_orphan_subtree",
        bash_command=RANK3_ORPHAN_SUBTREE_CMD,
        retries=0,
        max_active_tis_per_dag=40,
    ).expand(env=[{} for _ in range(FANOUT)])

    # -- Combine Rank 2 + Rank 3 in one TI: a task that BOTH leaves an orphan
    # grandchild AND ignores TERM, so the timeout escalation races the surviving
    # subtree. This is the stacked worst case.
    @task(
        task_id="rank2_plus_rank3_combined",
        execution_timeout=timedelta(seconds=25),
        retries=0,
        max_active_tis_per_dag=10,
    )
    def combined(i: int) -> int:
        import subprocess

        # Leave a disowned grandchild (Rank 3) ...
        subprocess.Popen(
            "setsid sleep 300 & disown 2>/dev/null; "
            "nohup sleep 120 >/dev/null 2>&1 & disown 2>/dev/null; exit 0",
            shell=True,
            executable="/bin/bash",
        )
        # ... then ignore TERM and block past execution_timeout (Rank 2) so the
        # supervisor must SIGKILL us while the grandchild is still alive.
        subprocess.run(
            ["bash", "-c", "trap '' TERM INT; sleep 3600"],
            check=False,
        )
        return i

    combined_tasks = combined.expand(i=list(range(40)))

    # No inter-task deps: all three fan-outs run concurrently to maximize the
    # overlap of kills, orphans, and fork churn on the single worker.
    [partial_kill, orphan_subtree, combined_tasks]


slotleak_v3()
