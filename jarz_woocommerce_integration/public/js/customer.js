frappe.ui.form.on('Customer', {
    refresh(frm) {
        // Show WooCommerce outbound status as a page indicator
        const status = frm.doc.woo_outbound_status;
        if (status) {
            const color_map = {
                'Synced': 'green',
                'Error': 'red',
                'Pending': 'orange',
                'Skipped': 'grey',
            };
            frm.page.set_indicator(
                __('WooCommerce: {0}', [status]),
                color_map[status] || 'grey'
            );
        }

        // Add "Push to WooCommerce" button (always visible)
        const btn = frm.add_custom_button(__('Push to WooCommerce'), function () {
            frappe.confirm(
                __('Push this Customer to WooCommerce now? This will override any pending or failed sync.'),
                function () {
                    frappe.show_alert({ message: __('Pushing to WooCommerce…'), indicator: 'blue' });
                    frappe.call({
                        method: 'jarz_woocommerce_integration.api.manual_sync.push_customer',
                        args: { customer_name: frm.doc.name },
                        callback(r) {
                            if (r.exc) return;
                            const result = r.message || {};
                            if (result.status === 'ok') {
                                frappe.show_alert({
                                    message: __('Pushed successfully (Woo Customer #{0})', [result.woo_customer_id || '?']),
                                    indicator: 'green',
                                });
                            } else if (result.skipped) {
                                frappe.show_alert({
                                    message: __('Skipped — {0}', [result.reason || 'already in sync']),
                                    indicator: 'blue',
                                });
                            } else if (result.status === 'error') {
                                frappe.show_alert({
                                    message: __('Sync error: {0}', [result.detail]),
                                    indicator: 'red',
                                });
                            }
                            frm.reload_doc();
                        },
                    });
                }
            );
        }, __('WooCommerce'));

        // Highlight the button red when status is Error so it stands out
        if (status === 'Error') {
            $(btn).addClass('btn-danger').removeClass('btn-default');
        }
    },
});
