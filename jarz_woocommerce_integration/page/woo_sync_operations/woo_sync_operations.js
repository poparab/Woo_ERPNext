frappe.provide("jarz_woocommerce_integration");

const WOO_SYNC_PAGE_OPEN_STATUSES = ["Pending", "RetryScheduled", "Processing"];
const WOO_SYNC_PAGE_RETRYABLE_STATUSES = ["Failed", "NeedsReview", "DeadLetter"];

frappe.pages["woo-sync-operations"].on_page_load = function (wrapper) {
	if (!wrapper.wooSyncOperationsPage) {
		wrapper.wooSyncOperationsPage = new jarz_woocommerce_integration.WooSyncOperationsPage(wrapper);
	}
};

frappe.pages["woo-sync-operations"].on_page_show = function (wrapper) {
	if (wrapper.wooSyncOperationsPage) {
		wrapper.wooSyncOperationsPage.refresh();
	}
};

jarz_woocommerce_integration.WooSyncOperationsPage = class WooSyncOperationsPage {
	constructor(wrapper) {
		this.wrapper = wrapper;
		this.ensure_styles();
		this.page = frappe.ui.make_app_page({
			parent: wrapper,
			title: __("Woo Sync Operations"),
			single_column: true,
		});
		frappe.breadcrumbs.add("Jarz WooCommerce Integration");
		this.windowField = this.page.add_field({
			label: __("Window (Hours)"),
			fieldname: "window_hours",
			fieldtype: "Select",
			options: ["6", "24", "72", "168"].join("\n"),
			default: "24",
			change: () => this.refresh(),
		});
		this.limitField = this.page.add_field({
			label: __("Rows"),
			fieldname: "row_limit",
			fieldtype: "Select",
			options: ["10", "20", "50"].join("\n"),
			default: "20",
			change: () => this.refresh(),
		});
		this.page.set_primary_action(__("Refresh"), () => this.refresh(), "refresh");
		this.page.set_secondary_action(__("Run Worker"), () => this.runWorker(), "play");
		this.page.add_menu_item(__("Clear Outbound Breaker"), () => this.clearBreaker());
		this.page.add_menu_item(__("Open Event List"), () => frappe.set_route("List", "WooCommerce Sync Event"));
		this.page.add_menu_item(__("Open Dashboard Report"), () => frappe.set_route("query-report", "Woo Sync Event Dashboard"));
		this.body = $("<div class='woo-sync-ops'></div>").appendTo(this.page.main);
		this.bind_events();
		this.refresh();
	}

	ensure_styles() {
		if (document.getElementById("woo-sync-operations-style")) {
			return;
		}
		const style = document.createElement("style");
		style.id = "woo-sync-operations-style";
		style.textContent = `
			.woo-sync-ops {
				display: grid;
				gap: 16px;
			}
			.woo-sync-ops__hero,
			.woo-sync-ops__panel {
				background: var(--fg-color);
				border: 1px solid var(--border-color);
				border-radius: var(--border-radius-md);
				padding: 16px;
			}
			.woo-sync-ops__hero {
				display: grid;
				gap: 12px;
				background: linear-gradient(135deg, rgba(17, 104, 108, 0.08), rgba(0, 0, 0, 0));
			}
			.woo-sync-ops__hero-title {
				display: flex;
				justify-content: space-between;
				gap: 12px;
				align-items: start;
			}
			.woo-sync-ops__hero-title h3 {
				margin: 0;
				font-size: 20px;
			}
			.woo-sync-ops__hero-meta {
				color: var(--text-muted);
				font-size: 12px;
			}
			.woo-sync-ops__cards {
				display: grid;
				grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
				gap: 12px;
			}
			.woo-sync-ops__card {
				background: var(--subtle-fg);
				border-radius: var(--border-radius-md);
				padding: 14px;
				border: 1px solid rgba(0, 0, 0, 0.04);
			}
			.woo-sync-ops__card-label {
				font-size: 12px;
				color: var(--text-muted);
				margin-bottom: 6px;
			}
			.woo-sync-ops__card-value {
				font-size: 26px;
				font-weight: 700;
			}
			.woo-sync-ops__card-note {
				font-size: 12px;
				color: var(--text-muted);
				margin-top: 6px;
			}
			.woo-sync-ops__chip-row {
				display: flex;
				flex-wrap: wrap;
				gap: 8px;
			}
			.woo-sync-ops__chip,
			.woo-sync-ops__badge {
				display: inline-flex;
				align-items: center;
				gap: 6px;
				padding: 6px 10px;
				border-radius: 999px;
				font-size: 12px;
				line-height: 1;
			}
			.woo-sync-ops__chip {
				background: var(--subtle-fg);
			}
			.woo-sync-ops__chip--on,
			.woo-sync-ops__badge--green {
				background: rgba(40, 167, 69, 0.12);
				color: #1f7a36;
			}
			.woo-sync-ops__chip--off,
			.woo-sync-ops__badge--gray {
				background: rgba(108, 117, 125, 0.12);
				color: #5f6770;
			}
			.woo-sync-ops__badge--orange {
				background: rgba(255, 153, 0, 0.14);
				color: #9a5b00;
			}
			.woo-sync-ops__badge--red {
				background: rgba(220, 53, 69, 0.14);
				color: #a61e2d;
			}
			.woo-sync-ops__badge--blue {
				background: rgba(0, 123, 255, 0.12);
				color: #0f5db8;
			}
			.woo-sync-ops__grid {
				display: grid;
				grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr);
				gap: 16px;
			}
			.woo-sync-ops__panel h4 {
				margin: 0 0 12px;
				font-size: 15px;
			}
			.woo-sync-ops__table {
				width: 100%;
				border-collapse: collapse;
			}
			.woo-sync-ops__table th,
			.woo-sync-ops__table td {
				padding: 10px 8px;
				border-top: 1px solid var(--border-color);
				vertical-align: top;
				font-size: 12px;
			}
			.woo-sync-ops__table th {
				border-top: none;
				color: var(--text-muted);
				font-weight: 600;
				padding-top: 0;
			}
			.woo-sync-ops__actions {
				display: flex;
				flex-wrap: wrap;
				gap: 6px;
			}
			.woo-sync-ops__action {
				border: 1px solid var(--border-color);
				background: transparent;
				border-radius: 999px;
				padding: 5px 9px;
				font-size: 11px;
			}
			.woo-sync-ops__muted {
				color: var(--text-muted);
			}
			.woo-sync-ops__empty {
				color: var(--text-muted);
				padding: 8px 0 0;
			}
			.woo-sync-ops__pillars {
				display: grid;
				grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
				gap: 12px;
			}
			@media (max-width: 991px) {
				.woo-sync-ops__grid {
					grid-template-columns: 1fr;
				}
			}
		`;
		document.head.appendChild(style);
	}

	bind_events() {
		this.body.on("click", "[data-action]", (event) => {
			const button = $(event.currentTarget);
			const action = button.attr("data-action");
			const eventName = button.attr("data-event-name");
			if (!action || !eventName) {
				return;
			}
			if (action === "open-event") {
				frappe.set_route("Form", "WooCommerce Sync Event", eventName);
				return;
			}
			if (action === "open-local") {
				frappe.set_route("Form", button.attr("data-local-doctype"), button.attr("data-local-docname"));
				return;
			}
			if (action === "run-now") {
				this.callAction(
					"jarz_woocommerce_integration.api.sync_events.process_event_now",
					{ event_name: eventName },
					__("Event dispatched"),
					__("Processing event...")
				);
				return;
			}
			if (action === "retry") {
				this.callAction(
					"jarz_woocommerce_integration.api.sync_events.retry_event",
					{ event_name: eventName },
					__("Event reset for retry"),
					__("Retrying event...")
				);
				return;
			}
			if (action === "review") {
				this.promptReview(eventName, button.attr("data-review-state") || "Investigating");
				return;
			}
			if (action === "keep") {
				this.promptRetention(eventName);
				return;
			}
			if (action === "clear-keep") {
				this.callAction(
					"jarz_woocommerce_integration.api.sync_events.set_retention_policy",
					{ event_name: eventName, retain: 0 },
					__("Retention override cleared"),
					__("Clearing retention override...")
				);
			}
		});
	}

	escapeHtml(value) {
		return frappe.utils.escape_html(value == null ? "" : String(value));
	}

	statusTone(status) {
		const toneMap = {
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
		return toneMap[status] || "gray";
	}

	formatDateTime(value) {
		if (!value) {
			return "-";
		}
		try {
			return frappe.datetime.str_to_user(value);
		} catch (error) {
			return this.escapeHtml(value);
		}
	}

	renderBadge(text, tone) {
		return `<span class="woo-sync-ops__badge woo-sync-ops__badge--${tone}">${this.escapeHtml(text)}</span>`;
	}

	renderCards(data) {
		const cards = [
			{ label: __("Due Now"), value: data.backlog.due_now, note: data.backlog.oldest_due ? __("Oldest: {0}", [this.formatDateTime(data.backlog.oldest_due)]) : __("No due backlog") },
			{ label: __("Needs Attention"), value: data.backlog.needs_attention, note: __("Failed, review, and dead-letter") },
			{ label: __("Processing"), value: data.backlog.processing, note: __("Claimed by active worker") },
			{ label: __("Expired Locks"), value: data.backlog.expired_processing, note: __("Ready for stale-lock recovery") },
			{ label: __("Inbound"), value: data.direction_counts.Inbound || 0, note: __("Window total") },
			{ label: __("Outbound"), value: data.direction_counts.Outbound || 0, note: __("Window total") },
		];
		return cards
			.map((card) => {
				return `
					<div class="woo-sync-ops__card">
						<div class="woo-sync-ops__card-label">${this.escapeHtml(card.label)}</div>
						<div class="woo-sync-ops__card-value">${this.escapeHtml(card.value)}</div>
						<div class="woo-sync-ops__card-note">${card.note}</div>
					</div>
				`;
			})
			.join("");
	}

	renderSettings(settings) {
		// The three master outbound kill-switches come FIRST and deliberately so.
		// They gate work before any of the ledger flags below are even consulted,
		// so with one of them off the rest of this row can be entirely green while
		// nothing at all is being pushed. Staging sat exactly like that for seven
		// weeks (2026-06-12 to 2026-08-01); the API was taught to report them but
		// this panel still never drew them, so the blind spot survived the fix.
		const flags = [
			["enable_outbound_orders", __("Order Push")],
			["enable_outbound_customers", __("Customer Push")],
			["enable_outbound_tracking_url", __("Tracking Link Push")],
			["tracking_base_url_configured", __("Tracking Base URL")],
			["enabled", __("Ledger")],
			["worker_enabled", __("Worker")],
			["shadow_mode", __("Shadow Mode")],
			["use_outbox_for_customer_push", __("Customer Outbox")],
			["use_outbox_for_invoice_push", __("Invoice Outbox")],
			["use_inbox_for_order_webhook", __("Order Webhook Inbox")],
			["use_inbox_for_customer_webhook", __("Customer Webhook Inbox")],
			["use_inbox_for_order_polling", __("Order Polling Inbox")],
			["use_event_reconciliation", __("Reconciliation")],
		];
		return flags
			.map(([key, label]) => {
				const value = settings[key];
				const klass = value ? "woo-sync-ops__chip--on" : "woo-sync-ops__chip--off";
				return `<span class="woo-sync-ops__chip ${klass}">${this.escapeHtml(label)}: ${value ? __("On") : __("Off")}</span>`;
			})
			.join("");
	}

	renderCountChips(title, items, tone) {
		const keys = Object.keys(items || {});
		if (!keys.length) {
			return `
				<div class="woo-sync-ops__panel">
					<h4>${this.escapeHtml(title)}</h4>
					<div class="woo-sync-ops__empty">${__("No data in the selected window")}</div>
				</div>
			`;
		}
		const chips = keys
			.map((key) => `<span class="woo-sync-ops__chip ${tone ? `woo-sync-ops__badge--${tone}` : ""}">${this.escapeHtml(key)}: ${this.escapeHtml(items[key])}</span>`)
			.join("");
		return `
			<div class="woo-sync-ops__panel">
				<h4>${this.escapeHtml(title)}</h4>
				<div class="woo-sync-ops__chip-row">${chips}</div>
			</div>
		`;
	}

	renderTable(rows, title) {
		if (!rows || !rows.length) {
			return `
				<div class="woo-sync-ops__panel">
					<h4>${this.escapeHtml(title)}</h4>
					<div class="woo-sync-ops__empty">${__("No events found")}</div>
				</div>
			`;
		}

		const body = rows.map((row) => this.renderRow(row)).join("");
		return `
			<div class="woo-sync-ops__panel">
				<h4>${this.escapeHtml(title)}</h4>
				<table class="woo-sync-ops__table">
					<thead>
						<tr>
							<th>${__("Event")}</th>
							<th>${__("Flow")}</th>
							<th>${__("Status")}</th>
							<th>${__("Attempts")}</th>
							<th>${__("When")}</th>
							<th>${__("Problem")}</th>
							<th>${__("Actions")}</th>
						</tr>
					</thead>
					<tbody>${body}</tbody>
				</table>
			</div>
		`;
	}

	renderRow(row) {
		const actions = [];
		actions.push(`<button class="woo-sync-ops__action" data-action="open-event" data-event-name="${this.escapeHtml(row.name)}">${__("Open")}</button>`);
		if (row.local_doctype && row.local_docname) {
			actions.push(
				`<button class="woo-sync-ops__action" data-action="open-local" data-event-name="${this.escapeHtml(row.name)}" data-local-doctype="${this.escapeHtml(row.local_doctype)}" data-local-docname="${this.escapeHtml(row.local_docname)}">${__("Local")}</button>`
			);
		}
		if (WOO_SYNC_PAGE_OPEN_STATUSES.indexOf(row.status) !== -1) {
			actions.push(`<button class="woo-sync-ops__action" data-action="run-now" data-event-name="${this.escapeHtml(row.name)}">${__("Run")}</button>`);
		}
		if (WOO_SYNC_PAGE_RETRYABLE_STATUSES.indexOf(row.status) !== -1) {
			actions.push(`<button class="woo-sync-ops__action" data-action="retry" data-event-name="${this.escapeHtml(row.name)}">${__("Retry")}</button>`);
			actions.push(
				`<button class="woo-sync-ops__action" data-action="review" data-event-name="${this.escapeHtml(row.name)}" data-review-state="${this.escapeHtml(row.review_state || "Investigating")}">${__("Review")}</button>`
			);
		}
		if (row.is_retention_exempt) {
			actions.push(`<button class="woo-sync-ops__action" data-action="clear-keep" data-event-name="${this.escapeHtml(row.name)}">${__("Unkeep")}</button>`);
		} else {
			actions.push(`<button class="woo-sync-ops__action" data-action="keep" data-event-name="${this.escapeHtml(row.name)}">${__("Keep")}</button>`);
		}

		const problem = row.manual_review_reason || row.last_error || "-";
		const whenValue = row.next_attempt_at || row.first_seen_on || row.modified;
		const statusCell = [this.renderBadge(row.status, this.statusTone(row.status))];
		if (row.review_state) {
			statusCell.push(this.renderBadge(row.review_state, "blue"));
		}
		if (row.is_retention_exempt) {
			statusCell.push(this.renderBadge(__("Kept"), "gray"));
		}

		return `
			<tr>
				<td>
					<div><strong>${this.escapeHtml(row.name)}</strong></div>
					<div class="woo-sync-ops__muted">${this.escapeHtml(row.object_type || "-")} / ${this.escapeHtml(row.source_id || "-")}</div>
				</td>
				<td>
					<div>${this.escapeHtml(row.direction || "-")}</div>
					<div class="woo-sync-ops__muted">${this.escapeHtml(row.event_type || "-")}</div>
				</td>
				<td><div class="woo-sync-ops__actions">${statusCell.join("")}</div></td>
				<td>${this.escapeHtml(`${row.attempt_count || 0}/${row.max_attempts || 0}`)}</td>
				<td>${this.formatDateTime(whenValue)}</td>
				<td>${this.escapeHtml(problem)}</td>
				<td><div class="woo-sync-ops__actions">${actions.join("")}</div></td>
			</tr>
		`;
	}

	refresh() {
		frappe.call({
			method: "jarz_woocommerce_integration.api.sync_events.get_dashboard",
			args: {
				window_hours: this.windowField.get_value(),
				limit: this.limitField.get_value(),
			},
			freeze: true,
			freeze_message: __("Loading Woo sync operations..."),
			callback: (r) => {
				if (r.exc) {
					return;
				}
				this.render(r.message || {});
			},
		});
	}

	render(data) {
		const breakerOpen = data.breaker && data.breaker.is_open;
		if (breakerOpen) {
			this.page.set_indicator(__("Breaker Open"), "red");
		} else if (data.backlog && data.backlog.needs_attention) {
			this.page.set_indicator(__("Attention Needed"), "orange");
		} else if (data.backlog && data.backlog.due_now) {
			this.page.set_indicator(__("Backlog Present"), "blue");
		} else {
			this.page.set_indicator(__("Healthy"), "green");
		}

		const breakerText = breakerOpen
			? __("Outbound circuit breaker open until {0}", [this.formatDateTime(data.breaker.open_until)])
			: __("Outbound circuit breaker is closed");
		const shadowFailures = data.shadow_failures || {};
		const shadowText = shadowFailures.count
			? __("Shadow insert failures in window: {0}", [shadowFailures.count])
			: __("No shadow insert failures in cache window");
		const generatedAt = this.formatDateTime(data.generated_at);

		this.body.html(`
			<div class="woo-sync-ops__hero">
				<div class="woo-sync-ops__hero-title">
					<div>
						<h3>${__("Live Ledger Control Surface")}</h3>
						<div class="woo-sync-ops__hero-meta">${__("Generated at {0}", [generatedAt])}</div>
					</div>
					<div class="woo-sync-ops__chip-row">
						${this.renderBadge(breakerText, breakerOpen ? "red" : "green")}
						${this.renderBadge(shadowText, shadowFailures.count ? "orange" : "gray")}
					</div>
				</div>
				<div class="woo-sync-ops__cards">${this.renderCards(data)}</div>
				<div class="woo-sync-ops__chip-row">${this.renderSettings(data.settings || {})}</div>
			</div>
			<div class="woo-sync-ops__pillars">
				${this.renderCountChips(__("Status Mix"), data.status_counts || {})}
				${this.renderCountChips(__("Review States"), data.review_state_counts || {}, "blue")}
				${this.renderCountChips(__("Top Event Types"), data.event_type_counts || {})}
			</div>
			<div class="woo-sync-ops__grid">
				${this.renderTable(data.attention_events || [], __("Attention Queue"))}
				${this.renderTable(data.recent_events || [], __("Recent Flow"))}
			</div>
		`);
	}

	callAction(method, args, successMessage, freezeMessage) {
		frappe.call({
			method: method,
			args: args,
			freeze: true,
			freeze_message: freezeMessage,
			callback: (r) => {
				if (r.exc) {
					return;
				}
				frappe.show_alert({ message: successMessage, indicator: "green" });
				this.refresh();
			},
		});
	}

	promptReview(eventName, defaultState) {
		frappe.prompt(
			[
				{
					fieldname: "review_state",
					label: __("Review State"),
					fieldtype: "Select",
					options: ["Open", "Investigating", "Resolved", "Ignored"].join("\n"),
					default: defaultState || "Investigating",
					reqd: 1,
				},
				{
					fieldname: "resolution_notes",
					label: __("Notes"),
					fieldtype: "Small Text",
				},
			],
			(values) => {
				this.callAction(
					"jarz_woocommerce_integration.api.sync_events.set_review_state",
					{
						event_name: eventName,
						review_state: values.review_state,
						resolution_notes: values.resolution_notes,
					},
					__("Review state updated"),
					__("Updating review state...")
				);
			},
			__("Set Review State"),
			__("Apply")
		);
	}

	promptRetention(eventName) {
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
				this.callAction(
					"jarz_woocommerce_integration.api.sync_events.set_retention_policy",
					{
						event_name: eventName,
						retain: 1,
						retain_days: values.retain_days,
					},
					__("Retention override applied"),
					__("Applying retention override...")
				);
			},
			__("Keep Event"),
			__("Apply")
		);
	}

	runWorker() {
		this.callAction(
			"jarz_woocommerce_integration.api.sync_events.run_worker",
			{},
			__("Worker run completed"),
			__("Running sync worker...")
		);
	}

	clearBreaker() {
		frappe.confirm(__("Clear the outbound circuit breaker and release deferred outbound events?"), () => {
			this.callAction(
				"jarz_woocommerce_integration.api.sync_events.clear_outbound_breaker",
				{},
				__("Outbound breaker cleared"),
				__("Clearing outbound breaker...")
			);
		});
	}
};