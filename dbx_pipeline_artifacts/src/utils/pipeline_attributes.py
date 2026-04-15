# Databricks notebook source
from pathlib import Path
import re


# COMMAND ----------

def get_pipeline_type(experiment_id):
    if experiment_id != 'prod':
        return 'Match_Maven'
    return 'Integration_Hub'


# COMMAND ----------

def get_pipeline_attributes():
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        print("No Databricks notebook context available — returning defaults for interactive mode")
        return None, None, None, None, "interactive"

    # --- 1. job_id and run_id directly from context ---
    job_id = ctx.jobId().get() if ctx.jobId().isDefined() else None
    run_id = ctx.jobRunId().get() if ctx.jobRunId().isDefined() else None

    # --- 2. task_key directly from context ---
    try:
        task_key = ctx.taskKey().get() if ctx.taskKey().isDefined() else None
    except Exception:
        task_key = None

    # --- 3. task_run_id and task_name via SDK using run_id ---
    task_run_id = None
    task_name = task_key  # default to context value

    if run_id:
        try:
            run = w.jobs.get_run(run_id)
            for t in run.tasks:
                # Match by task_key if available, else find the RUNNING task
                if task_key and t.task_key == task_key:
                    task_run_id = t.run_id
                    task_name = t.task_key
                    break
                elif not task_key and str(t.state.life_cycle_state) == "RunLifeCycleState.RUNNING":
                    task_run_id = t.run_id
                    task_name = t.task_key
                    break
        except Exception as e:
            print(f"Could not fetch job run details: {e}")

    return job_id, task_run_id, task_key, run_id, task_name