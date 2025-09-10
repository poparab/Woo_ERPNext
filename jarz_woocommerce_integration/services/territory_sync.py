from __future__ import annotations

from typing import Any, List

import frappe
import traceback

from jarz_woocommerce_integration.jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (  # standardized nested path
    WooCommerceSettings,
)
from jarz_woocommerce_integration.utils.http_client import WooClient

CODE_TO_DISPLAY = {
    "EGISM": "Ismailia - الاسماعيلية",
    "EG6OCT": "6 October - 6 أكتوبر",
    "EGZAYED": "Sheikh Zayed - الشيخ زايد",
    "EGHADAYEQOCT": "Hadayek October - حدائق أكتوبر",
    "EGHADAYEQAH": "Hadayek Al-Ahram - حدائق الاهرام",
    "EGFAYSAL": "Faisal - فيصل",
    "EGHARAM": "Haram - الهرم",
    "EGOMRANIYA": "Omraneya - العمرانيه",
    "EGGIZA": "Giza area - منطقة الجيزه",
    "EGMANIAL": "Manial - المنيل",
    "EGGARDENCITY": "Garden City - جاردن ستي",
    "EGMASROLD": "Old Cairo - مصر القديمه",
    "EGDOKKI": "Dokki - الدقي",
    "EGAWGAZA": "Agouza - العجوزه",
    "EGMOHANDSEEN": "Mohandessin - المهندسين",
    "EGZAMALEK": "Zamalek - الزمالك",
    "EGIMBABA": "Imbaba - امبابه",
    "EGBOLAK": "Boulak Al-Dakrour - بولاق الدكرور",
    "EGDOWNTOWN": "Downtown - وسط البلد",
    "EGSHOBRA": "Shubra - شبرا",
    "EGSHOBRAKH": "Shubra El-Kheima - شبرا الخيمه",
    "EGABBASSIYA": "Abbasiya - العباسيه",
    "EGHADAYEQ": "Hadayek El-Qobba - حدائق القبه",
    "EGZAWYA": "Zawya El-Hamra - الزاويه الحمرا",
    "EGMATARIYA": "Matareya - المطريه",
    "EGMASRJD": "Heliopolis - مصر الجديده",
    "EGNASRCITY": "Nasr City - مدينه نصر",
    "EGSALAM": "Alsalam City - مدينه السلام",
    "EGOBOUR": "Al Obour - العبور",
    "EGTAGAMMO": "Settlement - التجمع",
    "EGREHAB": "Rehab - الرحاب",
    "EGMADINATY": "Madinaty - مدينتي",
    "EGRSHEROUK": "Alsherouk - الشروق",
    "EGMAADI": "Maadi - المعادي",
    "EGMOQATTAM": "Moqattam - المقطم",
    "EGKATAMYA": "Katameya - القطاميه",
}


def build_client() -> WooClient:
    settings = WooCommerceSettings.get_settings()
    return WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.decrypted_consumer_secret,
        api_version=settings.api_version or "v3",
    )


def sync_territories() -> dict[str, Any]:
    """Pull delivery areas from Woo custom endpoint and create/update territories.

    Naming rules:
    - Root group: Egypt (is_group = 1)
    - Child territory name = "<English> / <Arabic>" if both present, else the one that exists.
        - Existing code-named territories keep their internal name (for stability) but we update
            their displayed title field (territory_name) to the bilingual form.

    NOTE: We originally attempted to store metadata in a description field, but the standard
    Territory DocType has no `description` column in this installation, so we rely solely on
    the territory_name itself for now. Stable unique key now uses custom field `custom_woo_code`.
    """
    client = build_client()
    areas = client.list_delivery_areas()
    if not areas:
        return {"areas": 0, "created": 0, "updated": 0}

    # Ensure Egypt territory
    # Some installations may not have a default_territory field (older/minimal setups)
    root_parent = "All Territories"
    try:
        val = frappe.db.get_single_value("Selling Settings", "default_territory")
        if val:
            root_parent = val
    except Exception:  # noqa: BLE001
        # Field missing; proceed with All Territories
        pass
    egypt_name = "Egypt"
    if not frappe.db.exists("Territory", egypt_name):
        root_doc = frappe.get_doc(
            {
                "doctype": "Territory",
                "territory_name": egypt_name,
                "is_group": 1,
                "parent_territory": root_parent,
            }
        )
        root_doc.insert(ignore_permissions=True)

    created = 0
    updated = 0
    woo_code_set = 0  # counts how many custom_woo_code values we set during this run
    errors: List[str] = []
    for area in areas:
        raw_code = (area.get("code") or "").strip()
        label_en = (area.get("en") or area.get("label") or raw_code).strip()
        label_ar = (area.get("ar") or "").strip()
        if not (label_en or label_ar):
            continue

        existing = None
        # 1. Primary match by custom_woo_code (stable even if territory_name renamed)
        if raw_code:
            existing = frappe.db.get_value("Territory", {"custom_woo_code": raw_code}, "name")
        # 2. Legacy fallback match by old woo_code field if present
        if not existing and raw_code:
            try:
                existing = frappe.db.get_value("Territory", {"woo_code": raw_code}, "name")
            except Exception:  # field might not exist
                existing = None
        # 3. Fallback to direct name (original code) if that territory still un-renamed
        if not existing and raw_code and frappe.db.exists("Territory", raw_code):
            existing = raw_code
        # 4. Fallback to English label if neither code-based variant found
        if not existing and label_en and frappe.db.exists("Territory", label_en):
            existing = label_en

        if existing:
            try:
                doc = frappe.get_doc("Territory", existing)
                # Only set custom_woo_code if missing/different; do not change territory_name
                if raw_code and getattr(doc, "custom_woo_code", None) != raw_code:
                    try:
                        doc.db_set("custom_woo_code", raw_code, commit=False)
                        woo_code_set += 1
                    except Exception:  # noqa: BLE001
                        pass
                # Keep structural consistency
                changed = False
                if doc.is_group != 0:
                    doc.is_group = 0
                    changed = True
                if doc.parent_territory != egypt_name:
                    doc.parent_territory = egypt_name
                    changed = True
                if changed:
                    doc.save(ignore_permissions=True)
                    updated += 1
            except Exception as e:  # noqa: BLE001
                errors.append(f"update_failed:{existing}:{e.__class__.__name__}")
                frappe.logger().error(
                    {
                        "event": "woo_territory_update_error",
                        "territory": existing,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )
            continue

        # Create new territory (use code as name if available else English or Arabic label)
        try:
            # Before creating, double-check not found via custom_woo_code (race condition)
            if raw_code:
                recheck = frappe.db.get_value("Territory", {"custom_woo_code": raw_code}, "name")
                if recheck:
                    existing = recheck
                    raise ValueError("already_exists_after_recheck")  # handled below
            new_doc = frappe.get_doc({
                "doctype": "Territory",
                "territory_name": raw_code or label_en or label_ar,
                "is_group": 0,
                "parent_territory": egypt_name,
            })
            new_doc.insert(ignore_permissions=True)
            if raw_code and hasattr(new_doc, "custom_woo_code"):
                try:
                    new_doc.db_set("custom_woo_code", raw_code, commit=False)
                    woo_code_set += 1
                except Exception:  # noqa: BLE001
                    pass
            created += 1
        except ValueError as ve:  # noqa: BLE001
            if str(ve) == "already_exists_after_recheck":
                # Treat as update (no creation), continue loop
                continue
            errors.append(f"create_failed:{raw_code or label_en}:{ve.__class__.__name__}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"create_failed:{raw_code or label_en}:{e.__class__.__name__}")
            frappe.logger().error(
                {
                    "event": "woo_territory_create_error",
                    "territory": raw_code or label_en,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }
            )

    try:
        frappe.db.commit()
    except Exception:  # noqa: BLE001
        errors.append("commit_failed")

    return {"areas": len(areas), "created": created, "updated": updated, "custom_woo_code_set": woo_code_set, "errors": errors}


def sync_territories_cron():  # pragma: no cover
    try:
        res = sync_territories()
        frappe.logger().info({"event": "woo_territory_sync", "result": res})
    except Exception:  # noqa: BLE001
        frappe.logger().error(
            {"event": "woo_territory_sync_error", "traceback": frappe.get_traceback()}
        )


def get_territories_missing_custom_woo_code() -> list[dict[str, Any]]:
    """Return territories (non-group) that have empty or null custom_woo_code."""
    fields = ["name", "territory_name", "parent_territory", "custom_woo_code"]
    return frappe.get_all(
        "Territory",
        filters={"is_group": 0, "custom_woo_code": ["in", ("", None)]},
        fields=fields,
    )


def populate_custom_woo_codes() -> dict[str, Any]:
    """Populate the custom field "custom_woo_code" for existing territories.

    Logic:
    - If a territory name matches a known code in CODE_TO_DISPLAY, set custom_woo_code to the name.
    - Skip if already set.
    Returns counts.
    """
    terrs = frappe.get_all(
        "Territory",
        filters={"is_group": 0},
        fields=["name", "custom_woo_code"],
    )
    updated = 0
    skipped = 0
    for t in terrs:
        if t.get("custom_woo_code"):
            skipped += 1
            continue
        name = t["name"]
        if name in CODE_TO_DISPLAY:
            frappe.db.set_value("Territory", name, "custom_woo_code", name, update_modified=False)
            updated += 1
        else:
            skipped += 1
    if updated:
        frappe.db.commit()
    return {"updated": updated, "skipped": skipped, "total": len(terrs)}