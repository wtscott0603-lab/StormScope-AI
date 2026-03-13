from __future__ import annotations

from fastapi import APIRouter

from backend.api.dependencies import get_frame_store
from backend.api.config import get_settings
from backend.api.schemas.radar import ProductResponse
from backend.shared.products import PRODUCT_CATALOG


router = APIRouter(tags=["products"])


@router.get("/api/products", response_model=list[ProductResponse])
@router.get("/api/v1/products", response_model=list[ProductResponse])
async def get_products() -> list[ProductResponse]:
    settings = get_settings()
    enabled = set(settings.enabled_products)
    store = get_frame_store()
    available_products: dict[str, bool] = {}
    for product_id in PRODUCT_CATALOG:
        available_products[product_id] = product_id in enabled and await store.product_has_frames(product_id)
    return [
        ProductResponse(
            id=product_id,
            name=meta["name"],
            description=meta["description"],
            unit=meta["unit"],
            enabled=product_id in enabled,
            available=available_products[product_id],
            source_kind=str(meta.get("source_kind", "raw")),
            source_product=str(meta["source_product"]) if meta.get("source_product") else None,
        )
        for product_id, meta in PRODUCT_CATALOG.items()
    ]
