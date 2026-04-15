import os
import re
import zipfile
import io
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.lakefusion_middlelayer_service.utils.app_db import token_required_wrapper
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

logs_router = APIRouter(tags=["Application Logs"], prefix='/logs')

# Log directory paths
# Deployed: /var/app_log/<service_name>/
# Local: ~/app_logs/<service_name>/
DEPLOYED_LOG_PATH = "/var/app_log"
LOCAL_LOG_PATH = os.path.expanduser("~/app_logs")


def get_log_base_path() -> str:
    """Get the base path for log files based on environment."""
    if os.path.exists(DEPLOYED_LOG_PATH) and os.path.isdir(DEPLOYED_LOG_PATH):
        return DEPLOYED_LOG_PATH
    return LOCAL_LOG_PATH


def get_service_log_path(service: str) -> Path:
    """Get the full path for a service's log directory."""
    base_path = get_log_base_path()
    return Path(base_path) / service


def parse_log_date(filename: str) -> Optional[datetime]:
    """Extract date from log filename like 'app-2026-01-20.log'."""
    match = re.match(r'app-(\d{4}-\d{2}-\d{2})\.log', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y-%m-%d')
    return None


# Pydantic models for request/response
class LogFileInfo(BaseModel):
    filename: str
    date: str
    size_bytes: int
    service: str


class ExportRequest(BaseModel):
    services: List[str]
    start_date: str
    end_date: str


@logs_router.get("/services", response_model=List[str])
def get_available_services(
    check: dict = Depends(token_required_wrapper)
):
    """Get list of available service log directories."""
    base_path = get_log_base_path()

    if not os.path.exists(base_path):
        logger.warning(f"Log base path does not exist: {base_path}")
        return []

    services = []
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        if os.path.isdir(item_path):
            services.append(item)

    return sorted(services)


@logs_router.get("/files", response_model=List[LogFileInfo])
def get_log_files(
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    check: dict = Depends(token_required_wrapper)
):
    """Get list of log files for a service within date range."""
    service_path = get_service_log_path(service)

    if not service_path.exists():
        raise HTTPException(status_code=404, detail=f"Service log directory not found: {service}")

    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    files = []
    for filename in os.listdir(service_path):
        if not filename.endswith('.log'):
            continue

        file_date = parse_log_date(filename)
        if file_date and start <= file_date <= end:
            file_path = service_path / filename
            files.append(LogFileInfo(
                filename=filename,
                date=file_date.strftime('%Y-%m-%d'),
                size_bytes=file_path.stat().st_size,
                service=service
            ))

    # Sort by date descending
    files.sort(key=lambda x: x.date, reverse=True)
    return files


@logs_router.get("/content")
def get_log_content(
    service: str = Query(..., description="Service name"),
    filename: str = Query(..., description="Log filename"),
    offset: int = Query(0, description="Line offset for pagination"),
    limit: int = Query(1000, description="Max lines to return"),
    check: dict = Depends(token_required_wrapper)
):
    """Get content of a specific log file with pagination."""
    service_path = get_service_log_path(service)
    file_path = service_path / filename

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Log file not found: {filename}")

    # Security: Ensure the resolved path is still within the service directory
    try:
        file_path.resolve().relative_to(service_path.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid filename")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()

        total_lines = len(lines)
        paginated_lines = lines[offset:offset + limit]

        return {
            "service": service,
            "filename": filename,
            "total_lines": total_lines,
            "offset": offset,
            "limit": limit,
            "has_more": offset + limit < total_lines,
            "content": "".join(paginated_lines)
        }
    except Exception as e:
        logger.error(f"Error reading log file {file_path}: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading log file: {str(e)}")


@logs_router.post("/export")
def export_logs(
    request: ExportRequest,
    check: dict = Depends(token_required_wrapper)
):
    """Export log files as a zip archive."""
    try:
        start = datetime.strptime(request.start_date, '%Y-%m-%d')
        end = datetime.strptime(request.end_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    # Create zip file in memory
    zip_buffer = io.BytesIO()
    files_added = 0

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for service in request.services:
            service_path = get_service_log_path(service)

            if not service_path.exists():
                logger.warning(f"Service log directory not found: {service}")
                continue

            for filename in os.listdir(service_path):
                if not filename.endswith('.log'):
                    continue

                file_date = parse_log_date(filename)
                if file_date and start <= file_date <= end:
                    file_path = service_path / filename
                    # Add file to zip with folder structure: service_name/filename
                    arcname = f"{service}/{filename}"
                    zip_file.write(file_path, arcname)
                    files_added += 1

    if files_added == 0:
        raise HTTPException(status_code=404, detail="No log files found matching the criteria")

    zip_buffer.seek(0)

    # Generate filename with date range
    zip_filename = f"logs_{request.start_date}_to_{request.end_date}.zip"

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename={zip_filename}"
        }
    )
