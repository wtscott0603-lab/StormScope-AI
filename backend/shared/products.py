from __future__ import annotations


PRODUCT_CATALOG: dict[str, dict[str, object]] = {
    "REF": {
        "id": "REF",
        "name": "Base Reflectivity",
        "description": "Lowest-tilt base reflectivity rendered from Level II data.",
        "unit": "dBZ",
        "field_aliases": ("reflectivity", "REF", "DBZ"),
        "source_kind": "raw",
        "source_product": None,
        "default_enabled": True,
    },
    "VEL": {
        "id": "VEL",
        "name": "Radial Velocity",
        "description": "Lowest-tilt radial velocity rendered from Level II data.",
        "unit": "m/s",
        "field_aliases": ("velocity", "VEL", "VR"),
        "source_kind": "raw",
        "source_product": None,
        "default_enabled": True,
    },
    "SRV": {
        "id": "SRV",
        "name": "Storm-Relative Velocity",
        "description": (
            "Radial velocity with a tracked-storm motion vector removed. "
            "Rendered from the same Level II volume as VEL."
        ),
        "unit": "m/s",
        "field_aliases": ("velocity", "VEL", "VR"),
        "source_kind": "derived",
        "source_product": "VEL",
        "default_enabled": True,
    },
    "CC": {
        "id": "CC",
        "name": "Correlation Coefficient",
        "description": "Dual-pol CC (rhohv) for debris and precipitation structure analysis.",
        "unit": "dimensionless",
        "field_aliases": ("cross_correlation_ratio", "CC", "RHOHV"),
        "source_kind": "raw",
        "source_product": None,
        "default_enabled": True,
    },
    "ZDR": {
        "id": "ZDR",
        "name": "Differential Reflectivity",
        "description": "Dual-pol ZDR for drop-shape and hail confirmation analysis.",
        "unit": "dB",
        "field_aliases": ("differential_reflectivity", "ZDR", "DIFFERENTIAL_REFLECTIVITY"),
        "source_kind": "raw",
        "source_product": None,
        "default_enabled": True,
    },
    "KDP": {
        "id": "KDP",
        "name": "Specific Differential Phase",
        "description": "Lowest-sweep KDP from the raw field when present, otherwise derived from differential phase as a proxy.",
        "unit": "deg/km",
        "field_aliases": ("specific_differential_phase", "KDP", "SPECIFIC_DIFFERENTIAL_PHASE"),
        "source_kind": "volume",
        "source_product": "REF",
        "default_enabled": True,
        "tilt_dependent": False,
    },
    "ET": {
        "id": "ET",
        "name": "Echo Tops",
        "description": "Volume-derived 18 dBZ echo-top height from the full reflectivity volume.",
        "unit": "km",
        "field_aliases": ("reflectivity", "REF", "DBZ"),
        "source_kind": "volume",
        "source_product": "REF",
        "default_enabled": True,
        "tilt_dependent": False,
    },
    "VIL": {
        "id": "VIL",
        "name": "Vertically Integrated Liquid",
        "description": "Volume-derived VIL estimate from full-volume reflectivity integration.",
        "unit": "kg/m²",
        "field_aliases": ("reflectivity", "REF", "DBZ"),
        "source_kind": "volume",
        "source_product": "REF",
        "default_enabled": True,
        "tilt_dependent": False,
    },
    "RR": {
        "id": "RR",
        "name": "Rain Rate",
        "description": "Reflectivity-based rain-rate estimate with conservative dual-pol adjustment when available.",
        "unit": "mm/h",
        "field_aliases": ("reflectivity", "REF", "DBZ"),
        "source_kind": "volume",
        "source_product": "REF",
        "default_enabled": True,
        "tilt_dependent": False,
    },
    "QPE1H": {
        "id": "QPE1H",
        "name": "1h QPE",
        "description": "Rolling 1-hour radar accumulation estimate derived from recent reflectivity scans.",
        "unit": "mm",
        "field_aliases": ("reflectivity", "REF", "DBZ"),
        "source_kind": "volume",
        "source_product": "REF",
        "default_enabled": True,
        "tilt_dependent": False,
    },
    "HC": {
        "id": "HC",
        "name": "Hydrometeor Class",
        "description": "Rules-based V1 hydrometeor interpretation from reflectivity and dual-pol context.",
        "unit": "class",
        "field_aliases": ("reflectivity", "REF", "DBZ"),
        "source_kind": "volume",
        "source_product": "REF",
        "default_enabled": True,
        "tilt_dependent": False,
    },
}


def product_ids() -> list[str]:
    return sorted(PRODUCT_CATALOG)


def product_meta(product: str) -> dict[str, object]:
    return PRODUCT_CATALOG[product.upper()]


def source_product_id(product: str) -> str:
    meta = product_meta(product)
    if meta.get("source_kind") != "derived":
        return product.upper()
    source_product = meta.get("source_product")
    return str(source_product or product).upper()


def is_derived_product(product: str) -> bool:
    return product_meta(product).get("source_kind") == "derived"


def is_volume_product(product: str) -> bool:
    return product_meta(product).get("source_kind") == "volume"


def is_raw_product(product: str) -> bool:
    return not is_derived_product(product) and not is_volume_product(product)


def product_is_tilt_dependent(product: str) -> bool:
    return bool(product_meta(product).get("tilt_dependent", True))


def raw_enabled_products(products: list[str]) -> list[str]:
    ordered: list[str] = []
    for product in products:
        product_id = product.upper()
        if product_id in PRODUCT_CATALOG and is_raw_product(product_id):
            ordered.append(product_id)
    return ordered
