function wooSyncEventListIndicator(doc) {
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
	const filter = ["status", "=", doc.status].join(",");
	let label = __(doc.status || "Unknown");
	if (doc.review_state) {
		label = __("{0} / {1}", [doc.status, doc.review_state]);
	}
	if (doc.is_retention_exempt) {
		label = __("{0} / Kept", [label]);
	}
	return [label, colorMap[doc.status] || "gray", filter];
}

function wooSyncEventSelectedNames(listview) {
	return listview.get_checked_items(true) || [];
}

function wooSyncEventRequireSelection(listview) {
	const names = wooSyncEventSelectedNames(listview);
	if (!names.length) {
		frappe.show_alert({ message: __("Select at least one event"), indicator: "orange" });
		return null;
	}
	return names;
}

function wooSyncEventBulkRetry(listview) {
	const names = wooSyncEventRequireSelection(listview);
	if (!names) {
		return;
	}
	frappe.call({
		method: "jarz_woocommerce_integration.api.sync_events.retry_events",
		args: { event_names: JSON.stringify(names) },
		freeze: true,
		freeze_message: __("Retrying selected events..."),
		callback(r) {
			if (r.exc) {
				return;
			}
			const count = (r.message && r.message.count) || names.length;
			frappe.show_alert({
				message: __("Queued {0} events for retry", [count]),
				indicator: "green",
			});
			listview.refresh();
		},
	});
}

function wooSyncEventBulkReview(listview, reviewState) {
	const names = wooSyncEventRequireSelection(listview);
	if (!names) {
		return;
	}
	frappe.prompt(
		[
			{
				fieldname: "resolution_notes",
				label: __("Notes"),
				fieldtype: "Small Text",
			},
		],
		(values) => {
			frappe.call({
				method: "jarz_woocommerce_integration.api.sync_events.set_review_state_bulk",
				args: {
					event_names: JSON.stringify(names),
					review_state: reviewState,
					resolution_notes: values.resolution_notes,
				},
				freeze: true,
				freeze_message: __("Updating selected events..."),
				callback(r) {
					if (r.exc) {
						return;
					}
					const count = (r.message && r.message.count) || names.length;
					frappe.show_alert({
						message: __("Updated {0} events", [count]),
						indicator: "green",
					});
					listview.refresh();
				},
			});
		},
		__("Set Review State: {0}", [reviewState]),
		__("Apply")
	);
}

function wooSyncEventBulkRetention(listview, retain) {
	const names = wooSyncEventRequireSelection(listview);
	if (!names) {
		return;
	}
	if (!retain) {
		frappe.confirm(__("Clear retention override for selected events?"), () => {
			frappe.call({
				method: "jarz_woocommerce_integration.api.sync_events.set_retention_policy_bulk",
				args: {
					event_names: JSON.stringify(names),
					retain: 0,
				},
				freeze: true,
				freeze_message: __("Clearing retention override..."),
				callback(r) {
					if (r.exc) {
						return;
					}
					const count = (r.message && r.message.count) || names.length;
					frappe.show_alert({
						message: __("Cleared {0} events", [count]),
						indicator: "green",
					});
					listview.refresh();
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
				method: "jarz_woocommerce_integration.api.sync_events.set_retention_policy_bulk",
				args: {
					event_names: JSON.stringify(names),
					retain: 1,
					retain_days: values.retain_days,
				},
				freeze: true,
				freeze_message: __("Applying retention override..."),
				callback(r) {
					if (r.exc) {
						return;
					}
					const count = (r.message && r.message.count) || names.length;
					frappe.show_alert({
						message: __("Updated {0} events", [count]),
						indicator: "green",
					});
					listview.refresh();
				},
			});
		},
		__("Keep Selected Events"),
		__("Apply")
	);
}

frappe.listview_settings["WooCommerce Sync Event"] = {
	add_fields: ["status", "direction", "review_state", "is_retention_exempt"],
	get_indicator(doc) {
		return wooSyncEventListIndicator(doc);
	},
	onload(listview) {
		listview.page.add_menu_item(__("Open Operations Dashboard"), () => frappe.set_route("woo-sync-operations"));
		listview.page.add_menu_item(__("Open Dashboard Report"), () => frappe.set_route("query-report", "Woo Sync Event Dashboard"));
		listview.page.add_action_item(__("Retry Selected"), () => wooSyncEventBulkRetry(listview));
		listview.page.add_action_item(__("Mark Investigating"), () => wooSyncEventBulkReview(listview, "Investigating"));
		listview.page.add_action_item(__("Mark Resolved"), () => wooSyncEventBulkReview(listview, "Resolved"));
		listview.page.add_action_item(__("Keep 30 Days"), () => wooSyncEventBulkRetention(listview, true));
		listview.page.add_action_item(__("Clear Keep"), () => wooSyncEventBulkRetention(listview, false));
	},
};