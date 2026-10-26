# Jarz Coupon Cap (WordPress plugin)

Companion to the ERPNext `Jarz Promo Code` -> WooCommerce coupon sync
(`services/coupon_sync.py`). The sync writes two coupon meta keys that
WooCommerce core cannot express; this plugin enforces them.

- `_jarz_max_discount` - caps the TOTAL discount of a percent coupon, in
  currency units (e.g. 20% off, but never more than 50).
- `_jarz_phone` - restricts the coupon to one customer by billing phone
  (customers with placeholder emails cannot be matched by email).

## Install

Zip the plugin so the folder `jarz-coupon-cap` is at the zip root
(`jarz-coupon-cap.zip` -> `jarz-coupon-cap/jarz-coupon-cap.php`, no extra
nesting), then WordPress admin -> Plugins -> Add New -> Upload Plugin ->
Activate.

## Notes

The cap applies only during cart calculation (it adjusts the discount amount
WooCommerce computes for the coupon). It does not alter stored coupons, past
orders, or the coupon's amount shown in the admin.
