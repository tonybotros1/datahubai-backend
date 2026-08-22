"""Compatibility entry point for the modular payroll-run package.

New payroll-run code lives in :mod:`app.routes.payroll_run`. Existing imports of
``app.routes.payroll_runs`` continue to work through this module.
"""

from app.routes.payroll_run import *
