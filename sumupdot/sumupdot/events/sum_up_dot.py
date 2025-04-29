import frappe
from datetime import datetime
from frappe.query_builder import DocType, functions as fn

@frappe.whitelist()
def sum_up_dot(**args):
    start_date = args.get('start_date')
    end_date = args.get('end_date')

    dot_sum = get_dot_sum(start_date, end_date)

    # Update over time values
    for row in dot_sum:
        if row["over_time"] > 0:
            frappe.db.set_value(
                "Employee",
                row["employee"],
                {
                    "custom_over_time": row["over_time"]
                }
            )
    frappe.db.commit()
    return frappe.msgprint(
        f"Updated Over Time in respective Employees successfully from {start_date} to {end_date}."
    )


def get_dot_sum(start_date, end_date):
    Timesheet = DocType("Timesheet")
    TimeLog = DocType("Timesheet Detail")

    # Join parent and child tables
    query = (
        frappe.qb.from_(Timesheet)
        .join(TimeLog).on(Timesheet.name == TimeLog.parent)
        .select(
            Timesheet.employee.as_("employee"),
            fn.Sum(TimeLog.over_time).as_("over_time")
        )
        .where(
            (Timesheet.docstatus == 1) &
            (Timesheet.start_date >= start_date) &
            (Timesheet.end_date <= end_date)
        )
        .groupby(Timesheet.employee)
        .having(fn.Sum(TimeLog.over_time) > 0)
    )

    return query.run(as_dict=True)