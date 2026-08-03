"""Builder for the WooCommerce Integration desk workspace.

The integration shipped with no workspace at all, so WooCommerce Settings, the
Woo Sync Operations page and the sync logs were reachable only by typing their
names into awesomebar — there was no entry for them anywhere in the Desk
sidebar.

Two surfaces, because Frappe v16 keeps them separate:

- the ``WooCommerce`` **Workspace** — the page itself, and
- a ``WooCommerce`` **section inside the Jarz POS Workspace Sidebar** — which is
  what actually makes Woo appear under Jarz in the nav rail.

``Workspace.parent_page`` is set too, but it does not drive sidebar grouping;
``sidebar.js: find_nested_items`` nests on Section Break + ``child`` rows in the
``Workspace Sidebar`` record. Setting only ``parent_page`` leaves Woo looking
like a separate top-level entry, which is exactly how this first shipped.

Both are soft references by name: this app imports nothing from ``jarz_pos``'s
business logic, and when the POS app is absent the workspace stands alone and
the sidebar append is skipped.

Same contract as the POS-side builder: the declarations below are the source of
truth and are rewritten on every ``bench migrate``, every target is
existence-checked, and any failure is logged rather than raised so a cosmetic
workspace problem cannot fail a migrate or a deploy.
"""

from __future__ import annotations

import json

import frappe

# ``Workspace`` autonames on ``field:label``, so this string is both the
# document name and the route (``/desk/woocommerce``).
WORKSPACE_NAME = "WooCommerce"
WORKSPACE_MODULE = "Jarz WooCommerce Integration"
WORKSPACE_ICON = "integration"
WORKSPACE_SEQUENCE = 2

# Nest under the POS workspace when it is present, else stand alone.
PARENT_WORKSPACE = "JARZ POS"

SHORTCUTS = [
    {"label": "Woo Sync Operations", "type": "Page", "link_to": "woo-sync-operations", "color": "#96588a", "icon": "integration"},
    {"label": "WooCommerce Settings", "type": "DocType", "link_to": "WooCommerce Settings", "color": "#34495e", "icon": "setting-gear"},
    {"label": "Sync Events", "type": "DocType", "link_to": "WooCommerce Sync Event", "color": "#2980b9", "icon": "list"},
    {"label": "Order Map", "type": "DocType", "link_to": "WooCommerce Order Map", "color": "#16a085", "icon": "file"},
]

CARDS = [
    {
        "label": "Sync Operations",
        "links": [
            {"label": "Woo Sync Operations", "link_type": "Page", "link_to": "woo-sync-operations"},
            {"label": "WooCommerce Sync Event", "link_type": "DocType", "link_to": "WooCommerce Sync Event"},
            {"label": "WooCommerce Sync Log", "link_type": "DocType", "link_to": "WooCommerce Sync Log"},
            {
                "label": "Woo Sync Event Dashboard",
                "link_type": "Report",
                "link_to": "Woo Sync Event Dashboard",
                "is_query_report": 1,
                "report_ref_doctype": "WooCommerce Sync Event",
            },
        ],
    },
    {
        "label": "Mapping & Bundles",
        "links": [
            {"label": "WooCommerce Order Map", "link_type": "DocType", "link_to": "WooCommerce Order Map"},
            {"label": "Woo Jarz Bundle", "link_type": "DocType", "link_to": "Woo Jarz Bundle"},
        ],
    },
    {
        "label": "Settings",
        "links": [
            {"label": "WooCommerce Settings", "link_type": "DocType", "link_to": "WooCommerce Settings"},
        ],
    },
]

_TARGET_DOCTYPE = {"DocType": "DocType", "Page": "Page", "Report": "Report"}

# The Jarz sidebar this app contributes a section to, referenced by document
# name only and reached through plain Frappe document APIs. No POS-app module is
# imported here — the two apps stay fully independent — so the row-building
# below is deliberately duplicated rather than shared, and every step degrades
# to a no-op when the record is absent.
JARZ_SIDEBAR = "Jarz POS"

SIDEBAR_SECTION_LABEL = "WooCommerce"
SIDEBAR_SECTION_LINKS = [
    {"label": "WooCommerce", "link_type": "Workspace", "link_to": WORKSPACE_NAME, "icon": WORKSPACE_ICON},
    {"label": "Woo Sync Operations", "link_type": "Page", "link_to": "woo-sync-operations"},
    {"label": "WooCommerce Settings", "link_type": "DocType", "link_to": "WooCommerce Settings"},
    {"label": "WooCommerce Sync Event", "link_type": "DocType", "link_to": "WooCommerce Sync Event"},
    {"label": "WooCommerce Sync Log", "link_type": "DocType", "link_to": "WooCommerce Sync Log"},
    {"label": "WooCommerce Order Map", "link_type": "DocType", "link_to": "WooCommerce Order Map"},
    {"label": "Woo Jarz Bundle", "link_type": "DocType", "link_to": "Woo Jarz Bundle"},
]


def ensure_woo_sidebar_section():  # pragma: no cover
    """Put a WooCommerce section inside the Jarz sidebar, so it nests under Jarz.

    A no-op when the Jarz sidebar record does not exist (POS app not installed,
    or an older version of it) — the standalone WooCommerce workspace still
    stands on its own in that case.

    Idempotent: an existing section with this label is dropped and rewritten, so
    repeated migrates never stack duplicates.
    """
    try:
        if not frappe.db.exists("Workspace Sidebar", JARZ_SIDEBAR):
            return

        links = [l for l in SIDEBAR_SECTION_LINKS if _sidebar_target_exists(l)]
        if not links:
            return

        sb = frappe.get_doc("Workspace Sidebar", JARZ_SIDEBAR)
        kept = _rows_without_our_section(sb)

        sb.set("items", [])
        for row in kept:
            sb.append("items", row)
        sb.append("items", {
            "type": "Section Break",
            "label": SIDEBAR_SECTION_LABEL,
            "child": 0,
            "collapsible": 1,
            # Collapsed by default so it does not push the POS sections off-screen.
            "keep_closed": 1,
        })
        for link in links:
            sb.append("items", {
                "type": "Link",
                "label": link["label"],
                "link_type": link["link_type"],
                "link_to": link.get("link_to"),
                "icon": link.get("icon"),
                # child=1 is what nests the row under the Section Break above it.
                "child": 1,
                "collapsible": 1,
                "indent": 0,
            })

        sb.flags.ignore_mandatory = True
        sb.flags.ignore_permissions = True
        sb.flags.ignore_links = True
        sb.save()
        frappe.db.commit()
    except Exception:  # noqa: BLE001
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "ensure_woo_sidebar_section failed")


def _rows_without_our_section(sb):
    """Existing rows minus our Section Break and the child rows beneath it."""
    kept, dropping = [], False
    for item in sb.items:
        if item.type == "Section Break":
            dropping = item.label == SIDEBAR_SECTION_LABEL
            if dropping:
                continue
        elif dropping and item.child:
            continue
        else:
            dropping = False
        kept.append({
            "type": item.type,
            "label": item.label,
            "link_type": item.link_type,
            "link_to": item.link_to,
            "url": item.url,
            "icon": item.icon,
            "child": item.child,
            "collapsible": item.collapsible,
            "indent": item.indent,
            "keep_closed": item.keep_closed,
        })
    return kept


def _sidebar_target_exists(link):
    """Sidebar links reach Workspaces too, which the card links cannot."""
    kind = link["link_type"]
    if kind == "Workspace":
        return bool(frappe.db.exists("Workspace", link["link_to"]))
    return _target_exists(kind, link["link_to"])


def ensure_woo_workspace():  # pragma: no cover
    """Create or rebuild the WooCommerce workspace. Called from ``after_migrate``."""
    try:
        shortcuts = [s for s in SHORTCUTS if _target_exists(s["type"], s["link_to"])]
        cards = _resolve_cards()

        if not shortcuts and not cards:
            return

        is_new = not frappe.db.exists("Workspace", WORKSPACE_NAME)
        ws = _load_or_new()
        ws.label = WORKSPACE_NAME
        ws.title = WORKSPACE_NAME
        ws.module = WORKSPACE_MODULE
        ws.icon = WORKSPACE_ICON
        ws.public = 1
        ws.is_hidden = 0
        ws.hide_custom = 0
        ws.sequence_id = WORKSPACE_SEQUENCE
        ws.parent_page = PARENT_WORKSPACE if frappe.db.exists("Workspace", PARENT_WORKSPACE) else ""

        ws.set("shortcuts", [])
        for sc in shortcuts:
            ws.append("shortcuts", {
                "label": sc["label"],
                "type": sc["type"],
                "link_to": sc["link_to"],
                "color": sc.get("color"),
                "icon": sc.get("icon"),
            })

        ws.set("links", [])
        for card in cards:
            ws.append("links", {
                "type": "Card Break",
                "label": card["label"],
                "link_count": len(card["links"]),
                "hidden": 0,
                "onboard": 0,
            })
            for link in card["links"]:
                ws.append("links", {
                    "type": "Link",
                    "label": link["label"],
                    "link_type": link["link_type"],
                    "link_to": link["link_to"],
                    "hidden": 0,
                    "onboard": 0,
                    "is_query_report": link.get("is_query_report", 0),
                    "report_ref_doctype": link.get("report_ref_doctype"),
                    "dependencies": "",
                })

        ws.content = json.dumps(_content_blocks(shortcuts, cards))
        ws.flags.ignore_mandatory = True
        ws.flags.ignore_permissions = True
        ws.flags.ignore_links = True

        if is_new:
            ws.insert(set_name=WORKSPACE_NAME)
        else:
            ws.save()

        frappe.db.commit()
    except Exception:  # noqa: BLE001
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "ensure_woo_workspace failed")


def _load_or_new():
    if frappe.db.exists("Workspace", WORKSPACE_NAME):
        return frappe.get_doc("Workspace", WORKSPACE_NAME)
    ws = frappe.new_doc("Workspace")
    ws.name = WORKSPACE_NAME
    return ws


def _target_exists(kind: str, name: str) -> bool:
    parent = _TARGET_DOCTYPE.get(kind)
    if not parent:
        return False
    try:
        return bool(frappe.db.exists(parent, name))
    except Exception:  # noqa: BLE001
        return False


def _resolve_cards():
    resolved = []
    for card in CARDS:
        links = [l for l in card["links"] if _target_exists(l["link_type"], l["link_to"])]
        if links:
            resolved.append({"label": card["label"], "links": links})
    return resolved


def _content_blocks(shortcuts, cards):
    blocks = [{
        "id": "woo_hdr",
        "type": "header",
        "data": {"text": '<span class="h4"><b>WooCommerce</b></span>', "col": 12},
    }]
    for i, sc in enumerate(shortcuts):
        blocks.append({
            "id": f"woo_sc_{i:02d}",
            "type": "shortcut",
            "data": {"shortcut_name": sc["label"], "col": 3},
        })
    if cards:
        blocks.append({
            "id": "woo_hdr_cards",
            "type": "header",
            "data": {"text": '<span class="h4"><b>Reports &amp; Masters</b></span>', "col": 12},
        })
        for i, card in enumerate(cards):
            blocks.append({
                "id": f"woo_card_{i:02d}",
                "type": "card",
                "data": {"card_name": card["label"], "col": 4},
            })
    return blocks
