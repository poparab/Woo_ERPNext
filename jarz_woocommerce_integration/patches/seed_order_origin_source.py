import frappe

from jarz_woocommerce_integration.services.outbound_sync import (
    _DEFAULT_ORDER_ORIGIN_SOURCE,
)


def execute():
    """One-time patch: write the order origin label into WooCommerce Settings.

    A ``default`` in a DocType JSON is applied by ``new_doc``, never by
    ``migrate``. A Single that already holds values is loaded straight out of
    ``tabSingles``, so a newly added field is simply absent from the row and
    reads back as ``None`` -- which left the operator looking at an empty
    "Order Origin Label" box while the pushed orders were in fact labelled from
    the module constant. The lever advertised in the field description did not
    exist until somebody saved the form once.

    Written with ``db.set_single_value`` rather than a full ``.save()`` on
    purpose: a full save of this Single validates every other field on it, and
    one stale Link value elsewhere on the doc would fail the whole migration.

    Idempotent, and never overwrites a value an operator has already chosen.
    """
    current = frappe.db.get_single_value("WooCommerce Settings", "order_origin_source")
    if (current or "").strip():
        return  # already configured -- the operator's choice wins

    frappe.db.set_single_value(
        "WooCommerce Settings", "order_origin_source", _DEFAULT_ORDER_ORIGIN_SOURCE
    )
    frappe.db.commit()
