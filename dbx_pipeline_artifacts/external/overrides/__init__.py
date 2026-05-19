"""
External Overrides for LakeFusion Task Executors.

This directory contains user-defined override classes that customize
the pre_execute() and post_execute() phases of task executors.

Folder Structure:
    Mirrors the executors/tasks/ folder structure for automatic discovery.
    No changes to notebooks required - paths are auto-inferred from executor class.

    external/overrides/
    ├── __init__.py
    ├── integration_core/
    │   ├── __init__.py
    │   ├── process_crosswalk_override.py.example
    │   ├── normal_deduplication/
    │   │   ├── __init__.py
    │   │   └── vector_search_override.py.example
    │   └── golden_deduplication/
    │       ├── __init__.py
    │       └── vector_search_override.py.example
    └── ...

Override Discovery:
    File naming: {task_name_snake_case}_override.py
    Class naming: {TaskNamePascalCase}Extended

    Auto-inference examples:
        Executor: ProcessCrosswalkTask (in tasks.integration_core.process_crosswalk)
        Override: external/overrides/integration_core/process_crosswalk_override.py
        Class:    ProcessCrosswalkExtended

        Executor: VectorSearchExecutor (in tasks.integration_core.normal_deduplication.vector_search)
        Override: external/overrides/integration_core/normal_deduplication/vector_search_override.py
        Class:    VectorSearchExtended

Override Modes:
    - Replace (default): Override methods run INSTEAD of default task methods
    - Extend (opt-in): Default methods run FIRST, then override methods run

    To enable extend mode, set class attributes on your override class:
        class ProcessCrosswalkExtended:
            extend_pre_execute = True   # Default pre_execute runs first, then yours
            extend_post_execute = True  # Default post_execute runs first, then yours

            def pre_execute(self, context):
                # Your additional logic here
                return context

            def post_execute(self, context, result):
                # Your additional logic here
                return result

Deployment Notes:
    - .py.example files are templates shipped with releases (never loaded as overrides)
    - .py files are actual customer overrides (loaded by the system)
    - New releases should only add/update .example files, never overwrite .py files
    - Customer customizations in .py files are preserved across releases

Note: The execute() phase (core business logic) cannot be overridden.
"""
