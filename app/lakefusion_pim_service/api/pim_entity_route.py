from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, get_db, token_required_wrapper
from lakefusion_utility.models.pim import (
    PimEntityCreate, PimEntityResponse, PimEntityDetailResponse,
    PimEntityListResponse, PimEntityEnrichedListResponse,
    PimEntityUpdate, PimPublishRequest,
    PimBatchValueWriteRequest, PimBatchValueResponse,
    PimBulkImportRequest, PimBulkImportResponse,
    PimFlatImportRequest, PimFlatImportResponse,
)
from app.lakefusion_pim_service.services.pim_entity_service import PimEntityService
from app.lakefusion_pim_service.services.pim_value_service import PimValueService
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List, Optional

app_logger = get_logger(__name__)


pim_entity_router = APIRouter(
    tags=["PIM Products"],
    prefix='/products',
)


# ===========================================================================
# PRODUCT CRUD  (SCRUM-1569, SCRUM-1573)
# ===========================================================================

@pim_entity_router.post("/", response_model=PimEntityResponse)
def create_product(
    entity_name: str,
    data: PimEntityCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.create_product(data)


@pim_entity_router.post("/import", response_model=PimBulkImportResponse)
def bulk_import(
    entity_name: str,
    data: PimBulkImportRequest,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Bulk import a full N-tier product hierarchy in a single transaction."""
    service = PimEntityService(db, entity_name)
    return service.bulk_import_hierarchy(data)


@pim_entity_router.post("/flat-import", response_model=PimFlatImportResponse)
def flat_import(
    entity_name: str,
    data: PimFlatImportRequest,
    db: Session = Depends(get_data_db),
    mysql_db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Flat-file import: item-grain rows upserted by SKU with per-field Add/Overwrite modes.

    On success, if the caller included the original file (file_name + file_content),
    it is archived to a UC Volume as part of this call (best-effort — archival
    failure does not fail the import)."""
    service = PimEntityService(db, entity_name)
    result = service.flat_import(data)
    # Archive the source file to a UC Volume only when the import succeeded (any rows
    # inserted/updated and not a total failure). Best-effort: never fail the import.
    if data.file_name and data.file_content and (result.get("inserted", 0) or result.get("updated", 0)):
        _archive_import_file(entity_name, data.file_name, data.file_content,
                             check.get("token", ""), mysql_db)
    return result


def _archive_import_file(entity_name: str, file_name: str, file_content: str, token: str, mysql_db: Session):
    """Upload the original import file to a UC Volume (PIM imports archive). Best-effort."""
    try:
        from io import BytesIO
        from datetime import datetime
        from lakefusion_utility.utils.databricks_util import CatalogService
        cat = CatalogService(token, mysql_db)
        # Timestamped path so re-imports don't clobber prior archives.
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        volume_path = (
            f"/Volumes/{cat.catalog_name}/metadata/metadata_files/"
            f"pim_imports/{entity_name}/{stamp}_{file_name}"
        )
        cat.w.files.upload(volume_path, BytesIO(file_content.encode("utf-8")), overwrite=True)
        app_logger.info(f"Archived PIM import file to {volume_path}")
    except Exception as e:
        app_logger.warning(f"PIM import file archival skipped (non-fatal): {e}")


@pim_entity_router.get("/", response_model=PimEntityEnrichedListResponse)
def list_products(
    entity_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    entity_type_id: Optional[str] = Query(None, description="Filter by entity type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    taxonomy_node_id: Optional[str] = Query(None, description="Filter by taxonomy node"),
    search: Optional[str] = Query(None, description="Search by SKU"),
    promoted: Optional[bool] = Query(None, description="Filter by promoted status (Live Catalog)"),
    sort_by: Optional[str] = Query(None, description="Sort column: sku, status, created_at, updated_at"),
    sort_dir: Optional[str] = Query("desc", description="Sort direction: asc or desc"),
    completeness_min: Optional[float] = Query(None, ge=0, le=100, description="Min completeness %"),
    completeness_max: Optional[float] = Query(None, ge=0, le=100, description="Max completeness %"),
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.list_products(
        page=page, page_size=page_size,
        entity_type_id=entity_type_id, status=status,
        taxonomy_node_id=taxonomy_node_id, search=search,
        promoted=promoted,
        sort_by=sort_by, sort_dir=sort_dir or 'desc',
        completeness_min=completeness_min, completeness_max=completeness_max,
    )


# ===========================================================================
# PUBLISH / UNPUBLISH  (SCRUM-1572)
# ===========================================================================

@pim_entity_router.post("/publish")
def publish_products(
    entity_name: str,
    data: PimPublishRequest,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.publish_products(data.product_ids)


@pim_entity_router.post("/unpublish")
def unpublish_products(
    entity_name: str,
    data: PimPublishRequest,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.unpublish_products(data.product_ids)


# ===========================================================================
# DASHBOARD — CATEGORY HEALTH  (SCRUM-1563)
# ===========================================================================

@pim_entity_router.get("/dashboard/stats")
def dashboard_stats(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.get_dashboard_stats()


@pim_entity_router.get("/dashboard/category-health")
def category_health(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.get_category_health()


@pim_entity_router.get("/dashboard/recent-activity")
def dashboard_recent_activity(
    limit: int = Query(20, ge=1, le=50),
    mysql_db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Pull recent PIM activity from audit logs."""
    from lakefusion_utility.models.auditlog import AuditLog
    from sqlalchemy import desc, or_

    pim_paths = ["/api/pim/products", "/api/pim/taxonomy", "/api/pim/attributes", "/api/pim/entity-bridge"]
    methods = ["POST", "PATCH", "DELETE"]

    logs = (
        mysql_db.query(AuditLog)
        .filter(
            AuditLog.request_method.in_(methods),
            AuditLog.response_status < 400,
            or_(*[AuditLog.api_path.like(f"{p}%") for p in pim_paths]),
        )
        .order_by(desc(AuditLog.created_date))
        .limit(limit)
        .all()
    )

    def describe(log):
        path = log.api_path or ""
        method = log.request_method or ""
        if "/products/publish" in path:
            return {"icon": "upload", "title": "Products published to Live Catalog"}
        if "/products" in path and "/items" in path:
            if method == "POST": return {"icon": "plus", "title": "Child item created"}
            if method == "DELETE": return {"icon": "trash", "title": "Child item deleted"}
        if "/products" in path and "/values" in path:
            return {"icon": "edit", "title": "Product attributes updated"}
        if "/products" in path:
            if method == "POST": return {"icon": "package", "title": "Product created"}
            if method == "PATCH": return {"icon": "edit", "title": "Product updated"}
            if method == "DELETE": return {"icon": "trash", "title": "Product deleted"}
        if "/taxonomy" in path:
            if method == "POST": return {"icon": "folder", "title": "Taxonomy node created"}
            if method == "PATCH": return {"icon": "edit", "title": "Taxonomy node updated"}
            if method == "DELETE": return {"icon": "trash", "title": "Taxonomy node deleted"}
        if "/attributes" in path:
            if method == "POST": return {"icon": "list", "title": "Attribute definition created"}
            if method == "PATCH": return {"icon": "edit", "title": "Attribute definition updated"}
        if "/entity-bridge" in path and "initialize" in path:
            return {"icon": "zap", "title": "PIM initialized"}
        if "/entity-bridge" in path and "sync-attribute" in path:
            return {"icon": "refresh", "title": "Global attribute synced from MDM"}
        return {"icon": "activity", "title": f"{method} {path.split('/api/pim/')[-1]}"}

    return [
        {
            **describe(log),
            "user": log.username or "system",
            "time": log.created_date.isoformat() if log.created_date else None,
            "method": log.request_method,
            "path": log.api_path,
        }
        for log in logs
    ]


# ===========================================================================
# BULK UPDATE VALUES  (SCRUM-1576)
# ===========================================================================

@pim_entity_router.patch("/bulk-values")
def bulk_update_values(
    entity_name: str,
    data: dict,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimValueService(db, entity_name)
    return service.bulk_update_values(data)


# ===========================================================================
# CSV EXPORT  (SCRUM-1585)
# ===========================================================================

@pim_entity_router.get("/export")
def export_products(
    entity_name: str,
    format: str = Query("csv", description="Export format: csv"),
    entity_type_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    taxonomy_node_id: Optional[str] = Query(None),
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.export_products_csv(
        entity_type_id=entity_type_id,
        status=status,
        taxonomy_node_id=taxonomy_node_id,
    )


@pim_entity_router.get("/{product_id}", response_model=PimEntityDetailResponse)
def get_product(
    entity_name: str,
    product_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.get_product(product_id)


@pim_entity_router.patch("/{product_id}", response_model=PimEntityResponse)
def update_product(
    entity_name: str,
    product_id: str,
    data: PimEntityUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.update_product(product_id, data)


@pim_entity_router.delete("/{product_id}")
def delete_product(
    entity_name: str,
    product_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.delete_product(product_id)


# ===========================================================================
# ITEM (SKU) CRUD  (SCRUM-1574)
# ===========================================================================

@pim_entity_router.post("/{product_id}/items", response_model=PimEntityResponse)
def create_item(
    entity_name: str,
    product_id: str,
    data: PimEntityCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.create_item(product_id, data)


@pim_entity_router.get("/{product_id}/items", response_model=List[PimEntityResponse])
def list_items(
    entity_name: str,
    product_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.list_items(product_id)


@pim_entity_router.patch("/{product_id}/items/{item_id}", response_model=PimEntityResponse)
def update_item(
    entity_name: str,
    product_id: str,
    item_id: str,
    data: PimEntityUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.update_item(product_id, item_id, data)


@pim_entity_router.delete("/{product_id}/items/{item_id}")
def delete_item(
    entity_name: str,
    product_id: str,
    item_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.delete_item(product_id, item_id)


# ===========================================================================
# ATTRIBUTE VALUES  (SCRUM-1575)
# ===========================================================================

@pim_entity_router.get("/{product_id}/values")
def get_values(
    entity_name: str,
    product_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimValueService(db, entity_name)
    return service.get_values_for_product(product_id)


@pim_entity_router.put("/{product_id}/values")
def batch_write_values(
    entity_name: str,
    product_id: str,
    data: PimBatchValueWriteRequest,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimValueService(db, entity_name)
    return service.batch_write_values(product_id, data)


@pim_entity_router.post("/{product_id}/values/delete-price-record")
def delete_price_record(
    entity_name: str,
    product_id: str,
    body: dict,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Delete a specific price record by composite key."""
    service = PimValueService(db, entity_name)
    return service.delete_price_record(
        product_id,
        body.get("attribute_id", ""),
        body.get("price_type", ""),
        body.get("currency", ""),
        body.get("territory", ""),
    )


@pim_entity_router.post("/{product_id}/values/delete-locale")
def delete_locale_values(
    entity_name: str,
    product_id: str,
    body: dict,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Delete all values for a product in a specific locale."""
    service = PimValueService(db, entity_name)
    return service.delete_locale_values(product_id, body.get("locale", ""))


@pim_entity_router.delete("/{product_id}/values/{attribute_id}")
def delete_value(
    entity_name: str,
    product_id: str,
    attribute_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimValueService(db, entity_name)
    return service.delete_value(product_id, attribute_id)


# ===========================================================================
# TRANSLATION  (SCRUM-1578)
# ===========================================================================

@pim_entity_router.post("/{product_id}/translate")
def translate_values(
    entity_name: str,
    product_id: str,
    data: dict,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Translate localizable attribute values from one locale to another using LLM.

    Body: {
        "source_locale": "en",
        "target_locale": "fr",
        "fields": [
            {"attribute_id": "abc", "value": "Premium cotton tee"},
            ...
        ]
    }
    Returns: translated fields with same structure.
    """
    import os
    from databricks.sdk import WorkspaceClient

    source_locale = data.get("source_locale", "en")
    target_locale = data.get("target_locale")
    fields = data.get("fields", [])

    if not target_locale or not fields:
        raise HTTPException(status_code=400, detail="target_locale and fields are required")

    # Get language names for better prompts
    from app.lakefusion_pim_service.utils import pim_sql
    lang_map = {
        l["id"]: l["language_name"]
        for l in pim_sql.fetch_all(db, f'SELECT "id", "language_name" FROM {pim_sql.pim_tbl(entity_name, "pim_language_ref")}')
    }
    source_lang = lang_map.get(source_locale, source_locale)
    target_lang = lang_map.get(target_locale, target_locale)

    # Build translation prompt
    texts_to_translate = []
    for f in fields:
        if f.get("value"):
            texts_to_translate.append(f)

    if not texts_to_translate:
        return {"translations": [], "model": "none", "message": "No values to translate"}

    # Format as numbered list for batch translation
    numbered = "\n".join([f"{i+1}. {t['value']}" for i, t in enumerate(texts_to_translate)])
    prompt = (
        f"Translate the following product information from {source_lang} to {target_lang}. "
        f"Return ONLY the translations as a numbered list in the same order, nothing else. "
        f"Keep product names, model numbers, and technical codes unchanged.\n\n{numbered}"
    )

    # Call Databricks serving endpoint — use model from request body, or fall back to env var
    model_name = data.get("model_name") or os.getenv("PIM_TRANSLATE_MODEL", "databricks-meta-llama-3-3-70b-instruct")
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_dapi = os.getenv("LAKEFUSION_DATABRICKS_DAPI")

    try:
        import requests as http_requests

        resp = http_requests.post(
            f"{databricks_host}/serving-endpoints/{model_name}/invocations",
            headers={"Authorization": f"Bearer {databricks_dapi}", "Content-Type": "application/json"},
            json={
                "messages": [
                    {"role": "system", "content": "You are a professional product content translator. Translate accurately and naturally."},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 2000,
                "temperature": 0.3,
            },
            timeout=60,
        )
        resp.raise_for_status()
        response = resp.json()

        raw_text = response["choices"][0]["message"]["content"].strip()
        lines = [l.strip() for l in raw_text.split("\n") if l.strip()]

        # Extract translations, removing numbering
        import re
        translated = []
        for line in lines:
            cleaned = re.sub(r'^\d+[\.\)]\s*', '', line).strip()
            if cleaned:
                translated.append(cleaned)

        # Map back to fields
        results = []
        for i, t in enumerate(texts_to_translate):
            results.append({
                "attribute_id": t["attribute_id"],
                "original": t["value"],
                "translated": translated[i] if i < len(translated) else t["value"],
                "locale": target_locale,
            })

        return {"translations": results, "model": model_name, "source": source_locale, "target": target_locale}

    except Exception as e:
        app_logger.error(f"Translation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Translation failed: {str(e)}")


# ===========================================================================
# COMPLETENESS  (SCRUM-1569)
# ===========================================================================

@pim_entity_router.get("/{product_id}/completeness")
def get_completeness(
    entity_name: str,
    product_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimEntityService(db, entity_name)
    return service.compute_completeness(product_id)
