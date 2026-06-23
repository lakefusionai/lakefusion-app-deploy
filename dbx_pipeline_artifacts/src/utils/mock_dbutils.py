"""
Mock dbutils for local VS Code development.

Run this cell before any Databricks notebook to simulate dbutils locally.
Provides: dbutils.widgets, dbutils.jobs.taskValues, dbutils.notebook

Usage:
    # As the first cell in any notebook:
    # MAGIC %run ../../utils/mock_dbutils

    # Or in VS Code, just run this file first, then run notebook cells.

Configure test values by editing MOCK_WIDGET_DEFAULTS and MOCK_TASK_VALUES below.
"""

# ── Test values — edit these for your test scenario ──────────────────

MOCK_WIDGET_DEFAULTS = {
    "catalog_name": "lakefusion_ai",
    "entity": "person",
    "experiment_id": "prod",
    "embedding_model": "databricks-gte-large-en",
    "llm_model": "databricks-meta-llama-3-1-70b-instruct",
    "llm_provisionless": "True",
    "embedding_endpoint": "",
    "vs_endpoint": "",
    "max_potential_matches": "5",
    "attributes": "[]",
    "embedding_model_source": "databricks_foundation",
    "process_records": "",
    "is_integration_hub": "",
}

MOCK_TASK_VALUES = {
    "Parse_Entity_Model_JSON": {
        "entity": "person",
        "embedding_model": "databricks-gte-large-en",
        "llm_model": "databricks-meta-llama-3-1-70b-instruct",
        "llm_provisionless": True,
        "catalog_name": "lakefusion_ai",
    },
}

# ── Mock implementation ──────────────────────────────────────────────


class _MockWidgets:
    def __init__(self):
        self._values = dict(MOCK_WIDGET_DEFAULTS)

    def text(self, name, default="", label=""):
        if name not in self._values:
            self._values[name] = default

    def dropdown(self, name, default, choices, label=""):
        if name not in self._values:
            self._values[name] = default

    def get(self, name):
        return self._values.get(name, "")


class _MockTaskValues:
    def __init__(self):
        self._store = {}

    def get(self, taskKey=None, key=None, debugValue=None, default=None, **kwargs):
        # Support both positional: get("Task", "key") and keyword: get(taskKey=..., key=...)
        if taskKey is None and key is None:
            # Positional args passed as (taskKey, key)
            return debugValue or default or ""

        task_vals = MOCK_TASK_VALUES.get(taskKey, {})
        val = task_vals.get(key)
        if val is not None:
            return val
        if debugValue is not None:
            return debugValue
        if default is not None:
            return default
        return self._store.get(f"{taskKey}.{key}", "")

    def set(self, key, value):
        self._store[key] = value
        print(f"  [mock] taskValues.set('{key}', {repr(value)[:80]})")


class _MockJobs:
    def __init__(self):
        self.taskValues = _MockTaskValues()


class _MockOptional:
    """Mimics Scala Option type used by notebook context."""
    def __init__(self, value=None):
        self._value = value

    def isDefined(self):
        return self._value is not None

    def get(self):
        return self._value


class _MockContext:
    def jobId(self):
        return _MockOptional(None)  # No job in local dev

    def runId(self):
        return _MockOptional(None)

    def taskKey(self):
        return _MockOptional(None)


class _MockNotebook:
    def getContext(self):
        return _MockContext()


class _MockGetDbutils:
    def notebook(self):
        return _MockNotebook()


class _MockEntryPoint:
    def getDbutils(self):
        return _MockGetDbutils()


class _MockNotebookUtils:
    entry_point = _MockEntryPoint()

    def exit(self, value):
        print(f"  [mock] notebook.exit({repr(value)[:120]})")


class MockDBUtils:
    def __init__(self):
        self.widgets = _MockWidgets()
        self.jobs = _MockJobs()
        self.notebook = _MockNotebookUtils()


# ── Inject into global scope ────────────────────────────────────────

try:
    dbutils  # noqa: F821 — already exists in Databricks runtime
    print("dbutils already available (Databricks runtime)")
except NameError:
    dbutils = MockDBUtils()  # noqa: F841
    print("Mock dbutils injected for local development")
