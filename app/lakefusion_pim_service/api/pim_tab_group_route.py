from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from lakefusion_utility.models.pim import PimTabGroup, PimTabGroupResponse
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List

app_logger = get_logger(__name__)

pim_tab_group_router = APIRouter(tags=["PIM Tab Groups"])


@pim_tab_group_router.get("/tab-groups", response_model=List[PimTabGroupResponse])
def list_tab_groups(
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """List predefined tab groups ordered by display_order.

    These are seeded on Initialize PIM and cannot be created by users. The
    pim_attribute_definition.group column references a tab group by name.
    """
    return db.query(PimTabGroup).order_by(PimTabGroup.display_order).all()
