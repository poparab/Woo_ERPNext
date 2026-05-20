const WOO_SYNC_EVENT_OPEN_STATUSES = ["Pending", "RetryScheduled", "Processing"];
const WOO_SYNC_EVENT_RETRYABLE_STATUSES = ["Failed", "NeedsReview", "DeadLetter"];

function wooSyncEventStatusColor(status) {
	const colorMap = {
		Pending: "orange",
		RetryScheduled: "orange",
		Processing: "blue",
		Succeeded: "green",
		Skipped: "gray",
		Superseded: "gray",
		Failed: "red",
		NeedsReview: "orange",
		DeadLetter: "red",
	};
	return colorMap[status] || "gray";
}

function wooSyncEventPromptReview(frm, defaultState) {
	frappe.prompt(
		[
			{
				fieldname: "review_state",
				label: __("Review State"),
				fieldtype: "Select",
				options: ["Open", "Investigating", "Resolved", "Ignored"].join("\n"),
				default: defaultState || frm.doc.review_state || "Investigating",
				reqd: 1,
			},
			{
				fieldname: "resolution_notes",
				label: __("Notes"),
				fieldtype: "Small Text",
				default: frm.doc.resolution_notes || "",
			},
		],
		(values) => {
			frappe.call({
				method: "jarz_woocommerce_integration.api.sync_events.set_review_state",
				args: {
					event_name: frm.doc.name,
					review_state: values.review_state,
					resolution_notes: values.resolution_notes,
				},
				freeze: true,
				freeze_message: __("Updating review state..."),
				callback(r) {
					if (r.exc) {
						return;
					}
					frappe.show_alert({
						message: __("Review state updated"),
						indicator: "green",
					});
					frm.reload_doc();
				},
			});
		},
		__("Set Review State"),
		__("Apply")
	);
}

function wooSyncEventSetRetention(frm, retain) {
	if (!retain) {
		frappe.confirm(__("Clear retention override for this event?"), () => {
			frappe.call({
				method: "jarz_woocommerce_integration.api.sync_events.set_retention_policy",
				args: {
					event_name: frm.doc.name,
					retain: 0,
				},
				freeze: true,
				freeze_message: __("Clearing retention override..."),
				callback(r) {
					if (r.exc) {
						return;
					}
					frappe.show_alert({
						message: __("Retention override cleared"),
						indicator: "green",
					});
					frm.reload_doc();
				},
			});
		});
		return;
	}

	frappe.prompt(
		[
			{
				fieldname: "retain_days",
				label: __("Retain For Days"),
				fieldtype: "Int",
				default: 30,
				reqd: 1,
			},
		],
		(values) => {
			frappe.call({
				method: "jarz_woocommerce_integration.api.sync_events.set_retention_policy",
				args: {
					event_name: frm.doc.name,
					retain: 1,
					retain_days: values.retain_days,
				},
				freeze: true,
				freeze_message: __("Applying retention override..."),
				callback(r) {
					if (r.exc) {
						return;
					}
					frappe.show_alert({
						message: __("Retention override applied"),
						indicator: "green",
					});
					frm.reload_doc();
				},
			});
		},
		__("Keep Event"),
		__("Apply")
	);
}

frappe.ui.form.on("WooCommerce Sync Event", {
	refresh(frm) {
		frm.page.set_indicator(__(frm.doc.status || "Unknown"), wooSyncEventStatusColor(frm.doc.status));

		frm.add_custom_button(
			__("Open Operations Dashboard"),
			() => frappe.set_route("woo-sync-operations"),
			__("WooCommerce")
		);

		if (frm.doc.local_doctype && frm.doc.local_docname) {
			frm.add_custom_button(
				__("Open Local Document"),
				() => frappe.set_route("Form", frm.doc.local_doctype, frm.doc.local_docname),
				__("WooCommerce")
			);
		}

		if (WOO_SYNC_EVENT_OPEN_STATUSES.includes(frm.doc.status)) {
			frm.add_custom_button(
				__("Process Now"),
				() => {
					frappe.call({
						method: "jarz_woocommerce_integration.api.sync_events.process_event_now",
						args: { event_name: frm.doc.name },
						freeze: true,
						freeze_message: __("Processing event..."),
						callback(r) {
							if (r.exc) {
								return;
							}
							frappe.show_alert({
								message: __("Event dispatched"),
								indicator: "green",
							});
							frm.reload_doc();
						},
					});
				},
				__("Actions")
			);
		}

		if (WOO_SYNC_EVENT_RETRYABLE_STATUSES.includes(frm.doc.status)) {
			frm.add_custom_button(
				__("Retry Event"),
				() => {
					frappe.call({
						method: "jarz_woocommerce_integration.api.sync_events.retry_event",
						args: { event_name: frm.doc.name },
						freeze: true,
						freeze_message: __("Retrying event..."),
						callback(r) {
							if (r.exc) {
								return;
							}
							frappe.show_alert({
								message: __("Event reset for retry"),
								indicator: "green",
							});
							frm.reload_doc();
						},
					});
				},
				__("Actions")
			);
		}

		frm.add_custom_button(__("Mark Investigating"), () => wooSyncEventPromptReview(frm, "Investigating"), __("Review"));
		frm.add_custom_button(__("Mark Resolved"), () => wooSyncEventPromptReview(frm, "Resolved"), __("Review"));
		frm.add_custom_button(__("Mark Ignored"), () => wooSyncEventPromptReview(frm, "Ignored"), __("Review"));

		if (frm.doc.is_retention_exempt) {
			frm.add_custom_button(__("Clear Keep"), () => wooSyncEventSetRetention(frm, false), __("Retention"));
		} else {
			frm.add_custom_button(__("Keep 30 Days"), () => wooSyncEventSetRetention(frm, true), __("Retention"));
		}
	},
});