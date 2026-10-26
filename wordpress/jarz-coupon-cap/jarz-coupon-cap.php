<?php
/**
 * Plugin Name: Jarz Coupon Cap
 * Description: Adds two coupon meta keys WooCommerce core lacks. _jarz_max_discount caps a percent coupon's total discount (currency units). _jarz_phone restricts a coupon to one customer by billing phone (for customers with placeholder emails). Set both through the REST API coupon meta_data.
 * Version: 0.2.0
 * Author: Jarz
 * Requires Plugins: woocommerce
 * License: GPL-2.0-or-later
 */

defined( 'ABSPATH' ) || exit;

/**
 * Running total of the discount already granted per coupon during one cart calculation.
 *
 * @return array<string,float>
 */
function &jarz_coupon_cap_running_totals() {
	static $totals = array();
	return $totals;
}

// WooCommerce applies a percent coupon item by item. Reset the tally around every cart calculation.
add_action(
	'woocommerce_before_calculate_totals',
	static function () {
		$totals = &jarz_coupon_cap_running_totals();
		$totals = array();
	},
	1
);

/**
 * Cap the TOTAL discount of a percent coupon at the coupon's _jarz_max_discount.
 *
 * Core calls this legacy filter once per cart line for percent coupons (WC_Discounts::apply_coupon_percent),
 * passing the line's discount in currency units. Because the value is changed, core also skips its
 * rounding "remainder" top-up, so the cap holds.
 *
 * @param float      $discount           Discount for this line.
 * @param float      $discounting_amount Price the percentage was applied to.
 * @param array|null $cart_item          Cart item.
 * @param bool       $single             Unused.
 * @param WC_Coupon  $coupon             Coupon being applied.
 * @return float
 */
function jarz_coupon_cap_filter_discount( $discount, $discounting_amount, $cart_item, $single, $coupon ) {
	if ( ! $coupon instanceof WC_Coupon || 'percent' !== $coupon->get_discount_type() ) {
		return $discount;
	}

	$cap = (float) $coupon->get_meta( '_jarz_max_discount', true );
	if ( $cap <= 0 ) {
		return $discount;
	}

	$totals = &jarz_coupon_cap_running_totals();
	$code   = $coupon->get_code();
	$so_far = isset( $totals[ $code ] ) ? $totals[ $code ] : 0.0;

	$allowed         = max( 0.0, $cap - $so_far );
	$discount        = min( (float) $discount, $allowed );
	$totals[ $code ] = $so_far + $discount;

	return $discount;
}
add_filter( 'woocommerce_coupon_get_discount_amount', 'jarz_coupon_cap_filter_discount', 20, 5 );

/**
 * Keep the last 10 digits of a phone number so +20 100 123 4567, 0100 123 4567 and 1001234567 compare equal.
 *
 * @param string $phone Raw phone.
 * @return string
 */
function jarz_coupon_cap_normalize_phone( $phone ) {
	$digits = preg_replace( '/\D+/', '', (string) $phone );
	return strlen( $digits ) > 10 ? substr( $digits, -10 ) : $digits;
}

/**
 * Billing phone known right now (customer session, or the checkout form being posted).
 *
 * @return string
 */
function jarz_coupon_cap_current_phone() {
	$phone = '';
	if ( function_exists( 'WC' ) && WC()->customer ) {
		$phone = (string) WC()->customer->get_billing_phone();
	}
	if ( '' === $phone && isset( $_POST['billing_phone'] ) ) { // phpcs:ignore WordPress.Security.NonceVerification.Missing
		$phone = sanitize_text_field( wp_unslash( $_POST['billing_phone'] ) ); // phpcs:ignore WordPress.Security.NonceVerification.Missing
	}
	return $phone;
}

/**
 * Reject a coupon that carries _jarz_phone when the billing phone differs or is missing.
 *
 * @param bool      $valid     Validity so far.
 * @param WC_Coupon $coupon    Coupon.
 * @param mixed     $discounts WC_Discounts instance.
 * @return bool
 * @throws Exception With the message shown to the shopper.
 */
function jarz_coupon_cap_validate_phone( $valid, $coupon, $discounts ) {
	if ( ! $valid || ! $coupon instanceof WC_Coupon ) {
		return $valid;
	}

	$required = jarz_coupon_cap_normalize_phone( $coupon->get_meta( '_jarz_phone', true ) );
	if ( '' === $required ) {
		return $valid;
	}

	// No phone yet (cart page, before checkout): accept now, the checkout gates below enforce it.
	$given = jarz_coupon_cap_normalize_phone( jarz_coupon_cap_current_phone() );
	if ( '' !== $given && $given !== $required ) {
		throw new Exception( esc_html__( 'This code is not valid for this phone number.', 'jarz-coupon-cap' ), 100 );
	}

	return $valid;
}
add_filter( 'woocommerce_coupon_is_valid', 'jarz_coupon_cap_validate_phone', 20, 3 );

/**
 * Last gate at checkout: the phone typed in the form must match, even if the coupon was applied earlier.
 *
 * @param array    $data   Posted checkout data.
 * @param WP_Error $errors Validation errors.
 */
function jarz_coupon_cap_checkout_phone_check( $data, $errors ) {
	if ( ! function_exists( 'WC' ) || ! WC()->cart ) {
		return;
	}
	$given = jarz_coupon_cap_normalize_phone( isset( $data['billing_phone'] ) ? $data['billing_phone'] : '' );

	foreach ( WC()->cart->get_applied_coupons() as $code ) {
		$coupon   = new WC_Coupon( $code );
		$required = jarz_coupon_cap_normalize_phone( $coupon->get_meta( '_jarz_phone', true ) );
		if ( '' !== $required && $required !== $given ) {
			$errors->add(
				'jarz_coupon_phone',
				/* translators: %s: coupon code. */
				sprintf( esc_html__( 'Coupon "%s" is not valid for this phone number.', 'jarz-coupon-cap' ), esc_html( $code ) )
			);
		}
	}
}
add_action( 'woocommerce_after_checkout_validation', 'jarz_coupon_cap_checkout_phone_check', 10, 2 );

/**
 * Same gate for the Block checkout (Store API), which skips woocommerce_after_checkout_validation.
 *
 * @param WC_Order        $order   Draft order built from the request.
 * @param WP_REST_Request $request Checkout request.
 * @throws \Automattic\WooCommerce\StoreApi\Exceptions\RouteException When the phone does not match.
 */
function jarz_coupon_cap_store_api_phone_check( $order, $request ) {
	$given = jarz_coupon_cap_normalize_phone( $order->get_billing_phone() );
	foreach ( $order->get_coupon_codes() as $code ) {
		$coupon   = new WC_Coupon( $code );
		$required = jarz_coupon_cap_normalize_phone( $coupon->get_meta( '_jarz_phone', true ) );
		if ( '' !== $required && $required !== $given ) {
			throw new \Automattic\WooCommerce\StoreApi\Exceptions\RouteException(
				'jarz_coupon_phone',
				/* translators: %s: coupon code. */
				sprintf( esc_html__( 'Coupon "%s" is not valid for this phone number.', 'jarz-coupon-cap' ), esc_html( $code ) ),
				400
			);
		}
	}
}
add_action( 'woocommerce_store_api_checkout_update_order_from_request', 'jarz_coupon_cap_store_api_phone_check', 10, 2 );
