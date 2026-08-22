"""MongoDB collections used by the payroll-run sections."""

from app.database import get_collection


payroll_runs_collection = get_collection("payroll_runs")


payroll_runs_employees_collection = get_collection("payroll_runs_employees")


payroll_runs_employees_elements_collection = get_collection("payroll_runs_employees_elements")


payroll_collection = get_collection("payroll")


companies_collection = get_collection("companies")


users_collection = get_collection("sys-users")


employees_email_collection = get_collection("employees_email")


company_mail_settings_collection = get_collection("company_mail_settings")


payroll_period_details_collection = get_collection("payroll_period_details")


leave_types_collection = get_collection("leave_types")


loan_and_advances_types_collection = get_collection("loan_and_advances_types")


employees_collection = get_collection("employees")


employees_payrolls_collection = get_collection("employees_payrolls")


payroll_elements_collection = get_collection("payroll_elements")


employees_leaves_collection = get_collection("employees_leaves")


employees_loan_and_advances_collection = get_collection("employees_loan_and_advances")


legislations_collection = get_collection("legislations")


payroll_elements_based_elements_collection = get_collection("payroll_elements_based_elements")
