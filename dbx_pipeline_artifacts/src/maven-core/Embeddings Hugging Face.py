# Databricks notebook source
# Install everything needed for embedding endpoint registration + serving.
# sentence-transformers is pinned to 4.x because v5+ rejects pandas Series in
# the mlflow flavor's predict path. The PyFunc wrapper makes this resilient,
# but keeping the notebook env on 4.x also prevents infer_signature issues.
# torch is pinned to a range available on PyPI (avoids "version not found"
# errors when the exact runtime torch version only ships from PyTorch's index).
# MAGIC %pip install --quiet --force-reinstall --no-deps "sentence-transformers==4.1.0"
# MAGIC %pip install --quiet "torch>=2.0,<2.6"
# MAGIC %pip install --quiet "transformers>=4.51.0,<5.0.0"
# MAGIC %pip install --quiet "mlflow[databricks]"
# MAGIC %pip install --quiet databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Fail fast if any core library is missing or mispinned.
# pip installs can silently fail behind VPN/firewall restrictions.
_required = ["torch", "transformers", "sentence_transformers", "mlflow"]
_missing = []
for _lib in _required:
    try:
        __import__(_lib)
    except ImportError:
        _missing.append(_lib)

if _missing:
    error_msg = (
        f"FATAL: Required libraries failed to install (possibly VPN/firewall restriction): "
        f"{', '.join(_missing)}. Check pip install output above for errors."
    )
    print(error_msg)
    dbutils.notebook.exit(error_msg)

import torch
import transformers
import sentence_transformers
import mlflow

print(f"torch:                 {torch.__version__}")
print(f"transformers:          {transformers.__version__}")
print(f"sentence-transformers: {sentence_transformers.__version__}")
print(f"mlflow:                {mlflow.__version__}")

assert sentence_transformers.__version__.startswith("4."), (
    f"sentence-transformers is {sentence_transformers.__version__}, need 4.x"
)

# COMMAND ----------

dbutils.widgets.text("embedding_model", "", "Embedding Model Hugging Face")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

embedding_model = dbutils.widgets.get("embedding_model")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

embedding_model = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "embedding_model", debugValue=embedding_model
)

# COMMAND ----------

embedding_model_endpoint = embedding_model.replace("/", "-").replace(".", "_").lower()
mlflow_model_name = f"{catalog_name}.embedding.{embedding_model_endpoint}"
print(embedding_model_endpoint, "|", embedding_model, "|", mlflow_model_name)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

tags = get_resource_tags() or {}

# COMMAND ----------

# ---- Version lookup -------------------------------------------------------
import builtins
import mlflow
from mlflow.exceptions import RestException

client = mlflow.MlflowClient()


def get_latest_version_or_none(model_name: str):
    """Return latest version string, or None if model/versions don't exist."""
    try:
        client.get_registered_model(model_name)
    except RestException as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e):
            return None
        raise
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        return None
    return builtins.max(versions, key=lambda mv: int(mv.version)).version


latest_version_model = get_latest_version_or_none(mlflow_model_name)

# COMMAND ----------

# ---- Registration (only if model doesn't exist) ---------------------------
if latest_version_model is None:
    import importlib.metadata as md
    import numpy as np
    import pandas as pd
    import mlflow.pyfunc
    from sentence_transformers import SentenceTransformer
    from mlflow.models import validate_serving_input
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import ColSpec, Schema, TensorSpec

    # Probe the model to validate it loads and capture embedding dim
    try:
        _probe = SentenceTransformer(embedding_model)
        embedding_dim = _probe.get_sentence_embedding_dimension()
        del _probe
    except Exception as e:
        raise RuntimeError(
            f"Failed to load HuggingFace model '{embedding_model}': {e}"
        ) from e

    class SentenceTransformerEmbedder(mlflow.pyfunc.PythonModel):
        """
        PyFunc wrapper that normalizes any input shape to list[str] before
        calling encode(). Sidesteps the mlflow.sentence_transformers flavor's
        serving predict, which fails on pandas Series with v5.
        """

        def __init__(self, model_name: str):
            self.model_name = model_name

        def load_context(self, context):
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(self.model_name)

        def predict(self, context, model_input, params=None):
            import numpy as np
            import pandas as pd

            if isinstance(model_input, pd.DataFrame):
                texts = model_input.iloc[:, 0].astype(str).tolist()
            elif isinstance(model_input, pd.Series):
                texts = model_input.astype(str).tolist()
            elif isinstance(model_input, np.ndarray):
                texts = [str(x) for x in model_input.tolist()]
            elif isinstance(model_input, str):
                texts = [model_input]
            elif isinstance(model_input, list):
                texts = [str(x) for x in model_input]
            elif isinstance(model_input, dict):
                val = next(iter(model_input.values()))
                texts = val if isinstance(val, list) else [val]
            else:
                texts = list(model_input)

            embeddings = self.model.encode(
                texts,
                batch_size=64,
                show_progress_bar=False,
                normalize_embeddings=True,
                convert_to_numpy=True,
            )
            return embeddings.astype(np.float32)

    # Explicit signature — required by Unity Catalog; avoids infer_signature
    # which also trips on v5 Series rejection.
    signature = ModelSignature(
        inputs=Schema([ColSpec("string")]),
        outputs=Schema([TensorSpec(np.dtype("float32"), (-1, embedding_dim))]),
    )
    sample_input = pd.DataFrame({"text": ["Hello, Dolly!"]})

    artifact_path = embedding_model.replace("/", "-").replace(".", "_").lower()

    # torch uses a range (not exact pin) because exact runtime versions can
    # ship only from PyTorch's index, not PyPI, causing "version not found"
    # errors at serving build time. sentence-transformers, transformers, and
    # mlflow pin exactly to match the notebook env.
    pip_requirements = [
        f"mlflow=={md.version('mlflow')}",
        "torch>=2.0,<2.6",
        f"transformers=={md.version('transformers')}",
        f"sentence-transformers=={md.version('sentence-transformers')}",
    ]

    with mlflow.start_run():
        model_info = mlflow.pyfunc.log_model(
            artifact_path=artifact_path,
            python_model=SentenceTransformerEmbedder(embedding_model),
            signature=signature,
            input_example=sample_input,
            registered_model_name=mlflow_model_name,
            pip_requirements=pip_requirements,
        )

    try:
        apply_tags_to_uc_resource("models", mlflow_model_name, tags)
    except Exception as e:
        logger.error(f"Tagging failed: {e}")

    # Validate the serving payload shape matches the signature
    validate_serving_input(
        model_info.model_uri,
        '{"dataframe_split": {"columns": ["text"], "data": [["Hello, Dolly!"]]}}',
    )

    latest_version_model = model_info.registered_model_version
    logger.info(f"Registered new model version: {latest_version_model}")
else:
    logger.info(f"Using existing model version: {latest_version_model}")

# COMMAND ----------

# ---- Serving endpoint ------------------------------------------------------
should_create = check_and_cleanup_failed_endpoint(embedding_model_endpoint)

try:
    if should_create:
        create_serving_endpoint(
            embedding_model_endpoint,
            mlflow_model_name,
            entity_version=latest_version_model,
        )
        logger.info(f"Endpoint '{embedding_model_endpoint}' is ready")
    else:
        logger.info(f"Endpoint '{embedding_model_endpoint}' already healthy")
except Exception as err:
    raise RuntimeError(
        f"Failed to create serving endpoint '{embedding_model_endpoint}': {err}"
    ) from err

# COMMAND ----------

dbutils.jobs.taskValues.set("served_entity", mlflow_model_name)
dbutils.jobs.taskValues.set("served_entity_version", latest_version_model)
dbutils.jobs.taskValues.set("embedding_model_endpoint", embedding_model_endpoint)

logger_instance.shutdown()
dbutils.notebook.exit({
    "served_entity": mlflow_model_name,
    "served_entity_version": latest_version_model,
    "embedding_model_endpoint": embedding_model_endpoint,
})
