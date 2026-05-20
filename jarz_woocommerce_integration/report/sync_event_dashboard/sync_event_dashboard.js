frappe.query_reports["Woo Sync Event Dashboard"] = {
	filters: [
		{
			fieldname: "from_date",
			label: __("From Date"),
			fieldtype: "Date",
			default: frappe.datetime.add_days(frappe.datetime.get_today(), -7),
			reqd: 1,
		},
		{
			fieldname: "to_date",
			label: __("To Date"),
			fieldtype: "Date",
			default: frappe.datetime.get_today(),
			reqd: 1,
		},
		{
			fieldname: "direction",
			label: __("Direction"),
			fieldtype: "Select",
			options: "\nInbound\nOutbound",
		},
		{
			fieldname: "status",
			label: __("Status"),
			fieldtype: "Select",
			options: "\nPending\nProcessing\nSucceeded\nRetryScheduled\nSkipped\nSuperseded\nFailed\nNeedsReview\nDeadLetter",
		},
		{
			fieldname: "review_state",
			label: __("Review State"),
			fieldtype: "Select",
			options: "\nOpen\nInvestigating\nResolved\nIgnored",
		},
		{
			fieldname: "event_type",
			label: __("Event Type"),
			fieldtype: "Data",
		},
		{
			fieldname: "object_type",
			label: __("Object Type"),
			fieldtype: "Data",
		},
	],
};