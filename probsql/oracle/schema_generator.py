"""
Schema Generator — Generates realistic database schemas across 10 domains.

Usage: python phase1_oracle/schema_generator.py

Outputs:
  - phase1_oracle/schemas/<domain>_NNN.json  (200 schema files)
  - phase1_oracle/schemas/_manifest.json     (index of all schemas)
"""

import json
import os
import random
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
SCHEMAS_DIR = SCRIPT_DIR / "schemas"

DOMAINS = {
    "ecommerce": {
        "tables": {
            "customers": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "updated_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "is_verified", "type": "BOOLEAN", "default": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "inactive", "suspended"]},
                    {"name": "lifetime_value", "type": "DECIMAL(10,2)", "default": 0},
                ],
                "optional_cols": [
                    {"name": "address", "type": "TEXT", "nullable": True},
                    {"name": "city", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "country", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "age", "type": "INT", "nullable": True},
                    {"name": "loyalty_points", "type": "INT", "default": 0},
                    {"name": "referral_code", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "last_login_at", "type": "TIMESTAMP", "nullable": True},
                ],
            },
            "orders": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "customer_id", "type": "INT", "foreign_key": "customers.id"},
                    {"name": "total_amount", "type": "DECIMAL(10,2)", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "processing", "shipped", "delivered", "cancelled", "refunded"]},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "shipped_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "delivered_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "shipping_address", "type": "TEXT", "nullable": True},
                    {"name": "discount_amount", "type": "DECIMAL(10,2)", "default": 0},
                    {"name": "payment_method", "type": "VARCHAR(30)", "enum_values": ["credit_card", "debit_card", "paypal", "bank_transfer", "cash"]},
                    {"name": "notes", "type": "TEXT", "nullable": True},
                    {"name": "is_gift", "type": "BOOLEAN", "default": False},
                    {"name": "tracking_number", "type": "VARCHAR(50)", "nullable": True},
                ],
            },
            "products": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "price", "type": "DECIMAL(10,2)", "nullable": False},
                    {"name": "category_id", "type": "INT", "foreign_key": "categories.id"},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "sku", "type": "VARCHAR(50)", "unique": True},
                    {"name": "stock_quantity", "type": "INT", "default": 0},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "weight", "type": "FLOAT", "nullable": True},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "review_count", "type": "INT", "default": 0},
                    {"name": "brand", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "discount_percent", "type": "FLOAT", "default": 0},
                ],
            },
            "categories": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "parent_id", "type": "INT", "nullable": True},
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "sort_order", "type": "INT", "default": 0},
                ],
            },
            "reviews": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "product_id", "type": "INT", "foreign_key": "products.id"},
                    {"name": "customer_id", "type": "INT", "foreign_key": "customers.id"},
                    {"name": "rating", "type": "INT", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "title", "type": "VARCHAR(200)", "nullable": True},
                    {"name": "body", "type": "TEXT", "nullable": True},
                    {"name": "is_verified_purchase", "type": "BOOLEAN", "default": False},
                    {"name": "helpful_count", "type": "INT", "default": 0},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "approved", "rejected"]},
                ],
            },
            "order_items": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "order_id", "type": "INT", "foreign_key": "orders.id"},
                    {"name": "product_id", "type": "INT", "foreign_key": "products.id"},
                    {"name": "quantity", "type": "INT", "nullable": False},
                    {"name": "unit_price", "type": "DECIMAL(10,2)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "discount_amount", "type": "DECIMAL(10,2)", "default": 0},
                    {"name": "total_price", "type": "DECIMAL(10,2)", "nullable": False},
                ],
            },
            "carts": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "customer_id", "type": "INT", "foreign_key": "customers.id"},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "updated_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "total_amount", "type": "DECIMAL(10,2)", "default": 0},
                ],
            },
        },
    },
    "saas": {
        "tables": {
            "users": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "role", "type": "VARCHAR(20)", "enum_values": ["admin", "member", "viewer", "owner"]},
                ],
                "optional_cols": [
                    {"name": "last_login_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "team_id", "type": "INT", "foreign_key": "teams.id"},
                    {"name": "avatar_url", "type": "VARCHAR(500)", "nullable": True},
                    {"name": "timezone", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "is_verified", "type": "BOOLEAN", "default": False},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                ],
            },
            "teams": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "owner_id", "type": "INT", "nullable": False},
                    {"name": "plan_id", "type": "INT", "foreign_key": "plans.id"},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "member_count", "type": "INT", "default": 1},
                    {"name": "billing_email", "type": "VARCHAR(255)", "nullable": True},
                ],
            },
            "subscriptions": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "team_id", "type": "INT", "foreign_key": "teams.id"},
                    {"name": "plan_id", "type": "INT", "foreign_key": "plans.id"},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "cancelled", "past_due", "trialing", "expired"]},
                    {"name": "started_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "cancelled_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "trial_ends_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "current_period_end", "type": "TIMESTAMP", "nullable": True},
                    {"name": "monthly_amount", "type": "DECIMAL(10,2)", "nullable": False},
                    {"name": "is_annual", "type": "BOOLEAN", "default": False},
                ],
            },
            "plans": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(50)", "nullable": False},
                    {"name": "price_monthly", "type": "DECIMAL(10,2)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "price_annual", "type": "DECIMAL(10,2)", "nullable": True},
                    {"name": "max_users", "type": "INT", "nullable": True},
                    {"name": "max_storage_gb", "type": "INT", "nullable": True},
                    {"name": "tier", "type": "VARCHAR(20)", "enum_values": ["free", "starter", "pro", "enterprise"]},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "features", "type": "TEXT", "nullable": True},
                ],
            },
            "usage_logs": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "user_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "action", "type": "VARCHAR(50)", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "team_id", "type": "INT", "foreign_key": "teams.id"},
                    {"name": "resource_type", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "resource_id", "type": "INT", "nullable": True},
                    {"name": "ip_address", "type": "VARCHAR(45)", "nullable": True},
                    {"name": "duration_ms", "type": "INT", "nullable": True},
                ],
            },
            "features": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "key", "type": "VARCHAR(50)", "nullable": False, "unique": True},
                ],
                "optional_cols": [
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "is_enabled", "type": "BOOLEAN", "default": True},
                    {"name": "min_plan_tier", "type": "VARCHAR(20)", "enum_values": ["free", "starter", "pro", "enterprise"]},
                ],
            },
        },
    },
    "healthcare": {
        "tables": {
            "patients": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "date_of_birth", "type": "DATE", "nullable": False},
                    {"name": "gender", "type": "VARCHAR(10)", "enum_values": ["male", "female", "other"]},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "address", "type": "TEXT", "nullable": True},
                    {"name": "blood_type", "type": "VARCHAR(5)", "enum_values": ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]},
                    {"name": "insurance_id", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "emergency_contact", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "allergies", "type": "TEXT", "nullable": True},
                ],
            },
            "doctors": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "specialization", "type": "VARCHAR(100)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "is_available", "type": "BOOLEAN", "default": True},
                    {"name": "years_experience", "type": "INT", "nullable": True},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "department", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "license_number", "type": "VARCHAR(50)", "nullable": True, "unique": True},
                ],
            },
            "appointments": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "patient_id", "type": "INT", "foreign_key": "patients.id"},
                    {"name": "doctor_id", "type": "INT", "foreign_key": "doctors.id"},
                    {"name": "scheduled_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["scheduled", "completed", "cancelled", "no_show"]},
                ],
                "optional_cols": [
                    {"name": "duration_minutes", "type": "INT", "default": 30},
                    {"name": "type", "type": "VARCHAR(30)", "enum_values": ["checkup", "follow_up", "emergency", "consultation", "procedure"]},
                    {"name": "notes", "type": "TEXT", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "completed_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "fee", "type": "DECIMAL(10,2)", "nullable": True},
                ],
            },
            "prescriptions": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "patient_id", "type": "INT", "foreign_key": "patients.id"},
                    {"name": "doctor_id", "type": "INT", "foreign_key": "doctors.id"},
                    {"name": "medication", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "prescribed_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "dosage", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "frequency", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "duration_days", "type": "INT", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "refills_remaining", "type": "INT", "default": 0},
                    {"name": "notes", "type": "TEXT", "nullable": True},
                ],
            },
            "diagnoses": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "patient_id", "type": "INT", "foreign_key": "patients.id"},
                    {"name": "doctor_id", "type": "INT", "foreign_key": "doctors.id"},
                    {"name": "condition", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "diagnosed_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "severity", "type": "VARCHAR(20)", "enum_values": ["mild", "moderate", "severe", "critical"]},
                    {"name": "icd_code", "type": "VARCHAR(10)", "nullable": True},
                    {"name": "is_chronic", "type": "BOOLEAN", "default": False},
                    {"name": "notes", "type": "TEXT", "nullable": True},
                    {"name": "resolved_at", "type": "TIMESTAMP", "nullable": True},
                ],
            },
            "lab_results": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "patient_id", "type": "INT", "foreign_key": "patients.id"},
                    {"name": "test_name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "result_value", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "tested_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "unit", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "reference_range", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "is_abnormal", "type": "BOOLEAN", "default": False},
                    {"name": "doctor_id", "type": "INT", "foreign_key": "doctors.id"},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "completed", "reviewed"]},
                ],
            },
        },
    },
    "finance": {
        "tables": {
            "accounts": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "account_number", "type": "VARCHAR(20)", "nullable": False, "unique": True},
                    {"name": "account_type", "type": "VARCHAR(20)", "enum_values": ["checking", "savings", "investment", "credit"]},
                    {"name": "balance", "type": "DECIMAL(15,2)", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "owner_name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "frozen", "closed"]},
                    {"name": "currency", "type": "VARCHAR(3)", "default": "USD"},
                    {"name": "is_verified", "type": "BOOLEAN", "default": False},
                    {"name": "credit_limit", "type": "DECIMAL(15,2)", "nullable": True},
                    {"name": "interest_rate", "type": "FLOAT", "nullable": True},
                    {"name": "last_activity_at", "type": "TIMESTAMP", "nullable": True},
                ],
            },
            "transactions": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "account_id", "type": "INT", "foreign_key": "accounts.id"},
                    {"name": "amount", "type": "DECIMAL(15,2)", "nullable": False},
                    {"name": "type", "type": "VARCHAR(20)", "enum_values": ["deposit", "withdrawal", "transfer", "payment", "fee", "interest"]},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "description", "type": "VARCHAR(500)", "nullable": True},
                    {"name": "category", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "completed", "failed", "reversed"]},
                    {"name": "reference_number", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "merchant_name", "type": "VARCHAR(200)", "nullable": True},
                    {"name": "is_recurring", "type": "BOOLEAN", "default": False},
                    {"name": "balance_after", "type": "DECIMAL(15,2)", "nullable": True},
                ],
            },
            "holdings": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "account_id", "type": "INT", "foreign_key": "accounts.id"},
                    {"name": "symbol", "type": "VARCHAR(10)", "nullable": False},
                    {"name": "quantity", "type": "DECIMAL(15,6)", "nullable": False},
                    {"name": "purchase_price", "type": "DECIMAL(15,2)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "current_price", "type": "DECIMAL(15,2)", "nullable": True},
                    {"name": "purchased_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "asset_type", "type": "VARCHAR(20)", "enum_values": ["stock", "bond", "etf", "mutual_fund", "crypto"]},
                    {"name": "unrealized_gain", "type": "DECIMAL(15,2)", "nullable": True},
                ],
            },
            "transfers": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "from_account_id", "type": "INT", "foreign_key": "accounts.id"},
                    {"name": "to_account_id", "type": "INT", "foreign_key": "accounts.id"},
                    {"name": "amount", "type": "DECIMAL(15,2)", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "completed", "failed", "cancelled"]},
                    {"name": "fee", "type": "DECIMAL(10,2)", "default": 0},
                    {"name": "notes", "type": "TEXT", "nullable": True},
                    {"name": "completed_at", "type": "TIMESTAMP", "nullable": True},
                ],
            },
            "beneficiaries": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "account_id", "type": "INT", "foreign_key": "accounts.id"},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "bank_name", "type": "VARCHAR(100)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "account_number", "type": "VARCHAR(30)", "nullable": False},
                    {"name": "is_verified", "type": "BOOLEAN", "default": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "last_used_at", "type": "TIMESTAMP", "nullable": True},
                ],
            },
        },
    },
    "hr": {
        "tables": {
            "employees": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "department_id", "type": "INT", "foreign_key": "departments.id"},
                    {"name": "hired_at", "type": "DATE", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "on_leave", "terminated", "probation"]},
                ],
                "optional_cols": [
                    {"name": "title", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "manager_id", "type": "INT", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "salary", "type": "DECIMAL(12,2)", "nullable": True},
                    {"name": "is_remote", "type": "BOOLEAN", "default": False},
                    {"name": "office_location", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "date_of_birth", "type": "DATE", "nullable": True},
                    {"name": "performance_rating", "type": "FLOAT", "nullable": True},
                ],
            },
            "departments": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "head_id", "type": "INT", "nullable": True},
                    {"name": "budget", "type": "DECIMAL(15,2)", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "location", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
            },
            "salaries": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "employee_id", "type": "INT", "foreign_key": "employees.id"},
                    {"name": "amount", "type": "DECIMAL(12,2)", "nullable": False},
                    {"name": "effective_date", "type": "DATE", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "currency", "type": "VARCHAR(3)", "default": "USD"},
                    {"name": "pay_frequency", "type": "VARCHAR(20)", "enum_values": ["monthly", "biweekly", "weekly"]},
                    {"name": "bonus", "type": "DECIMAL(12,2)", "default": 0},
                    {"name": "is_current", "type": "BOOLEAN", "default": True},
                ],
            },
            "leave_requests": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "employee_id", "type": "INT", "foreign_key": "employees.id"},
                    {"name": "start_date", "type": "DATE", "nullable": False},
                    {"name": "end_date", "type": "DATE", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "approved", "rejected", "cancelled"]},
                    {"name": "type", "type": "VARCHAR(20)", "enum_values": ["vacation", "sick", "personal", "maternity", "paternity", "bereavement"]},
                ],
                "optional_cols": [
                    {"name": "reason", "type": "TEXT", "nullable": True},
                    {"name": "approved_by", "type": "INT", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "days_count", "type": "INT", "nullable": False},
                ],
            },
            "performance_reviews": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "employee_id", "type": "INT", "foreign_key": "employees.id"},
                    {"name": "reviewer_id", "type": "INT", "foreign_key": "employees.id"},
                    {"name": "rating", "type": "FLOAT", "nullable": False},
                    {"name": "review_date", "type": "DATE", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "comments", "type": "TEXT", "nullable": True},
                    {"name": "goals_met", "type": "BOOLEAN", "default": False},
                    {"name": "period", "type": "VARCHAR(20)", "enum_values": ["Q1", "Q2", "Q3", "Q4", "annual"]},
                    {"name": "promotion_recommended", "type": "BOOLEAN", "default": False},
                ],
            },
            "roles": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "title", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "level", "type": "VARCHAR(20)", "enum_values": ["junior", "mid", "senior", "lead", "principal", "director", "vp"]},
                ],
                "optional_cols": [
                    {"name": "min_salary", "type": "DECIMAL(12,2)", "nullable": True},
                    {"name": "max_salary", "type": "DECIMAL(12,2)", "nullable": True},
                    {"name": "department_id", "type": "INT", "foreign_key": "departments.id"},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                ],
            },
        },
    },
    "education": {
        "tables": {
            "students": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "enrolled_at", "type": "DATE", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "graduated", "suspended", "withdrawn"]},
                ],
                "optional_cols": [
                    {"name": "gpa", "type": "FLOAT", "nullable": True},
                    {"name": "major", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "date_of_birth", "type": "DATE", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "year", "type": "INT", "nullable": True},
                    {"name": "is_international", "type": "BOOLEAN", "default": False},
                    {"name": "scholarship_amount", "type": "DECIMAL(10,2)", "default": 0},
                ],
            },
            "courses": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "code", "type": "VARCHAR(20)", "nullable": False, "unique": True},
                    {"name": "credits", "type": "INT", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "department", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "instructor_id", "type": "INT", "foreign_key": "instructors.id"},
                    {"name": "max_enrollment", "type": "INT", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "level", "type": "VARCHAR(20)", "enum_values": ["100", "200", "300", "400", "500", "600"]},
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "semester", "type": "VARCHAR(20)", "enum_values": ["fall", "spring", "summer"]},
                ],
            },
            "enrollments": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "student_id", "type": "INT", "foreign_key": "students.id"},
                    {"name": "course_id", "type": "INT", "foreign_key": "courses.id"},
                    {"name": "enrolled_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["enrolled", "dropped", "completed", "failed"]},
                ],
                "optional_cols": [
                    {"name": "grade", "type": "VARCHAR(2)", "nullable": True},
                    {"name": "semester", "type": "VARCHAR(20)", "enum_values": ["fall", "spring", "summer"]},
                    {"name": "year", "type": "INT", "nullable": True},
                ],
            },
            "grades": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "enrollment_id", "type": "INT", "foreign_key": "enrollments.id"},
                    {"name": "assignment_id", "type": "INT", "foreign_key": "assignments.id"},
                    {"name": "score", "type": "FLOAT", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "max_score", "type": "FLOAT", "default": 100},
                    {"name": "graded_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "feedback", "type": "TEXT", "nullable": True},
                    {"name": "is_late", "type": "BOOLEAN", "default": False},
                ],
            },
            "instructors": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "department", "type": "VARCHAR(100)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "title", "type": "VARCHAR(50)", "enum_values": ["professor", "associate_professor", "assistant_professor", "lecturer", "adjunct"]},
                    {"name": "hired_at", "type": "DATE", "nullable": True},
                    {"name": "is_tenured", "type": "BOOLEAN", "default": False},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                ],
            },
            "assignments": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "course_id", "type": "INT", "foreign_key": "courses.id"},
                    {"name": "title", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "due_date", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "max_score", "type": "FLOAT", "default": 100},
                    {"name": "type", "type": "VARCHAR(20)", "enum_values": ["homework", "quiz", "midterm", "final", "project", "lab"]},
                    {"name": "weight", "type": "FLOAT", "nullable": True},
                    {"name": "is_published", "type": "BOOLEAN", "default": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
            },
        },
    },
    "real_estate": {
        "tables": {
            "properties": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "address", "type": "TEXT", "nullable": False},
                    {"name": "city", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "property_type", "type": "VARCHAR(20)", "enum_values": ["house", "apartment", "condo", "townhouse", "commercial", "land"]},
                    {"name": "price", "type": "DECIMAL(15,2)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "bedrooms", "type": "INT", "nullable": True},
                    {"name": "bathrooms", "type": "FLOAT", "nullable": True},
                    {"name": "square_feet", "type": "INT", "nullable": True},
                    {"name": "year_built", "type": "INT", "nullable": True},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["available", "under_contract", "sold", "off_market"]},
                    {"name": "lot_size", "type": "FLOAT", "nullable": True},
                    {"name": "is_featured", "type": "BOOLEAN", "default": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "state", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "zip_code", "type": "VARCHAR(10)", "nullable": True},
                ],
            },
            "listings": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "property_id", "type": "INT", "foreign_key": "properties.id"},
                    {"name": "agent_id", "type": "INT", "foreign_key": "agents.id"},
                    {"name": "listing_price", "type": "DECIMAL(15,2)", "nullable": False},
                    {"name": "listed_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "pending", "sold", "expired", "withdrawn"]},
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "days_on_market", "type": "INT", "default": 0},
                    {"name": "views_count", "type": "INT", "default": 0},
                    {"name": "is_featured", "type": "BOOLEAN", "default": False},
                    {"name": "expires_at", "type": "TIMESTAMP", "nullable": True},
                ],
            },
            "agents": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "license_number", "type": "VARCHAR(50)", "unique": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "commission_rate", "type": "FLOAT", "nullable": True},
                    {"name": "total_sales", "type": "INT", "default": 0},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "joined_at", "type": "DATE", "nullable": False},
                ],
            },
            "viewings": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "listing_id", "type": "INT", "foreign_key": "listings.id"},
                    {"name": "scheduled_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "client_name", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "client_email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "agent_id", "type": "INT", "foreign_key": "agents.id"},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["scheduled", "completed", "cancelled", "no_show"]},
                    {"name": "feedback", "type": "TEXT", "nullable": True},
                    {"name": "interest_level", "type": "VARCHAR(10)", "enum_values": ["low", "medium", "high"]},
                ],
            },
            "offers": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "listing_id", "type": "INT", "foreign_key": "listings.id"},
                    {"name": "amount", "type": "DECIMAL(15,2)", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "buyer_name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "accepted", "rejected", "countered", "withdrawn"]},
                    {"name": "contingencies", "type": "TEXT", "nullable": True},
                    {"name": "expires_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "is_cash_offer", "type": "BOOLEAN", "default": False},
                ],
            },
            "tenants": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "property_id", "type": "INT", "foreign_key": "properties.id"},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "lease_start", "type": "DATE", "nullable": False},
                    {"name": "lease_end", "type": "DATE", "nullable": False},
                    {"name": "monthly_rent", "type": "DECIMAL(10,2)", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "deposit_amount", "type": "DECIMAL(10,2)", "nullable": True},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "expired", "terminated"]},
                ],
            },
        },
    },
    "social_media": {
        "tables": {
            "users": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "username", "type": "VARCHAR(50)", "nullable": False, "unique": True},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": False, "unique": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                ],
                "optional_cols": [
                    {"name": "display_name", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "bio", "type": "TEXT", "nullable": True},
                    {"name": "follower_count", "type": "INT", "default": 0},
                    {"name": "following_count", "type": "INT", "default": 0},
                    {"name": "post_count", "type": "INT", "default": 0},
                    {"name": "is_verified", "type": "BOOLEAN", "default": False},
                    {"name": "is_private", "type": "BOOLEAN", "default": False},
                    {"name": "last_active_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "location", "type": "VARCHAR(100)", "nullable": True},
                ],
            },
            "posts": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "user_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "content", "type": "TEXT", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "like_count", "type": "INT", "default": 0},
                    {"name": "comment_count", "type": "INT", "default": 0},
                    {"name": "share_count", "type": "INT", "default": 0},
                    {"name": "type", "type": "VARCHAR(20)", "enum_values": ["text", "image", "video", "link", "poll"]},
                    {"name": "is_pinned", "type": "BOOLEAN", "default": False},
                    {"name": "visibility", "type": "VARCHAR(20)", "enum_values": ["public", "private", "followers_only"]},
                    {"name": "updated_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "hashtags", "type": "TEXT", "nullable": True},
                ],
            },
            "comments": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "post_id", "type": "BIGINT", "foreign_key": "posts.id"},
                    {"name": "user_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "content", "type": "TEXT", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "like_count", "type": "INT", "default": 0},
                    {"name": "parent_id", "type": "BIGINT", "nullable": True},
                    {"name": "is_edited", "type": "BOOLEAN", "default": False},
                ],
            },
            "likes": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "user_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "post_id", "type": "BIGINT", "foreign_key": "posts.id"},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [],
            },
            "follows": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "follower_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "following_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "is_muted", "type": "BOOLEAN", "default": False},
                    {"name": "is_close_friend", "type": "BOOLEAN", "default": False},
                ],
            },
            "messages": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "sender_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "receiver_id", "type": "INT", "foreign_key": "users.id"},
                    {"name": "content", "type": "TEXT", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "is_read", "type": "BOOLEAN", "default": False},
                    {"name": "read_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "is_deleted", "type": "BOOLEAN", "default": False},
                ],
            },
        },
    },
    "logistics": {
        "tables": {
            "shipments": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "tracking_number", "type": "VARCHAR(50)", "nullable": False, "unique": True},
                    {"name": "origin", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "destination", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["created", "picked_up", "in_transit", "out_for_delivery", "delivered", "returned"]},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "carrier_id", "type": "INT", "foreign_key": "carriers.id"},
                    {"name": "weight_kg", "type": "FLOAT", "nullable": True},
                    {"name": "estimated_delivery", "type": "TIMESTAMP", "nullable": True},
                    {"name": "actual_delivery", "type": "TIMESTAMP", "nullable": True},
                    {"name": "shipping_cost", "type": "DECIMAL(10,2)", "nullable": True},
                    {"name": "is_fragile", "type": "BOOLEAN", "default": False},
                    {"name": "is_express", "type": "BOOLEAN", "default": False},
                    {"name": "warehouse_id", "type": "INT", "foreign_key": "warehouses.id"},
                ],
            },
            "warehouses": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "location", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "capacity", "type": "INT", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "current_utilization", "type": "FLOAT", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "manager_name", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "type", "type": "VARCHAR(20)", "enum_values": ["standard", "cold_storage", "hazmat", "bulk"]},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
            },
            "inventory": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "warehouse_id", "type": "INT", "foreign_key": "warehouses.id"},
                    {"name": "sku", "type": "VARCHAR(50)", "nullable": False},
                    {"name": "quantity", "type": "INT", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "product_name", "type": "VARCHAR(200)", "nullable": True},
                    {"name": "min_quantity", "type": "INT", "default": 10},
                    {"name": "max_quantity", "type": "INT", "nullable": True},
                    {"name": "last_restocked_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "unit_cost", "type": "DECIMAL(10,2)", "nullable": True},
                    {"name": "is_perishable", "type": "BOOLEAN", "default": False},
                    {"name": "expiry_date", "type": "DATE", "nullable": True},
                ],
            },
            "routes": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "origin", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "destination", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "distance_km", "type": "FLOAT", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "estimated_hours", "type": "FLOAT", "nullable": True},
                    {"name": "carrier_id", "type": "INT", "foreign_key": "carriers.id"},
                    {"name": "cost_per_kg", "type": "DECIMAL(10,2)", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "frequency", "type": "VARCHAR(20)", "enum_values": ["daily", "weekly", "biweekly", "monthly"]},
                ],
            },
            "carriers": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "type", "type": "VARCHAR(20)", "enum_values": ["ground", "air", "sea", "rail"]},
                ],
                "optional_cols": [
                    {"name": "contact_email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "on_time_percentage", "type": "FLOAT", "nullable": True},
                    {"name": "max_weight_kg", "type": "FLOAT", "nullable": True},
                ],
            },
            "tracking_events": {
                "base_cols": [
                    {"name": "id", "type": "BIGINT", "primary_key": True},
                    {"name": "shipment_id", "type": "INT", "foreign_key": "shipments.id"},
                    {"name": "status", "type": "VARCHAR(50)", "nullable": False},
                    {"name": "location", "type": "VARCHAR(200)", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "notes", "type": "TEXT", "nullable": True},
                    {"name": "is_exception", "type": "BOOLEAN", "default": False},
                ],
            },
        },
    },
    "restaurant": {
        "tables": {
            "restaurants": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "cuisine_type", "type": "VARCHAR(50)", "nullable": True},
                    {"name": "address", "type": "TEXT", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "price_range", "type": "VARCHAR(5)", "enum_values": ["$", "$$", "$$$", "$$$$"]},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "opening_time", "type": "VARCHAR(10)", "nullable": True},
                    {"name": "closing_time", "type": "VARCHAR(10)", "nullable": True},
                    {"name": "has_delivery", "type": "BOOLEAN", "default": False},
                    {"name": "has_takeout", "type": "BOOLEAN", "default": True},
                    {"name": "city", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                    {"name": "seating_capacity", "type": "INT", "nullable": True},
                ],
            },
            "menus": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "restaurant_id", "type": "INT", "foreign_key": "restaurants.id"},
                    {"name": "item_name", "type": "VARCHAR(200)", "nullable": False},
                    {"name": "price", "type": "DECIMAL(8,2)", "nullable": False},
                    {"name": "category", "type": "VARCHAR(50)", "enum_values": ["appetizer", "main", "dessert", "drink", "side", "special"]},
                ],
                "optional_cols": [
                    {"name": "description", "type": "TEXT", "nullable": True},
                    {"name": "is_available", "type": "BOOLEAN", "default": True},
                    {"name": "is_vegetarian", "type": "BOOLEAN", "default": False},
                    {"name": "is_vegan", "type": "BOOLEAN", "default": False},
                    {"name": "is_gluten_free", "type": "BOOLEAN", "default": False},
                    {"name": "calories", "type": "INT", "nullable": True},
                    {"name": "spice_level", "type": "VARCHAR(10)", "enum_values": ["mild", "medium", "hot", "extra_hot"]},
                    {"name": "preparation_time_min", "type": "INT", "nullable": True},
                ],
            },
            "orders": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "restaurant_id", "type": "INT", "foreign_key": "restaurants.id"},
                    {"name": "total_amount", "type": "DECIMAL(10,2)", "nullable": False},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["placed", "preparing", "ready", "picked_up", "delivered", "cancelled"]},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "customer_name", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "customer_phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "order_type", "type": "VARCHAR(20)", "enum_values": ["dine_in", "takeout", "delivery"]},
                    {"name": "tip_amount", "type": "DECIMAL(8,2)", "default": 0},
                    {"name": "discount_amount", "type": "DECIMAL(8,2)", "default": 0},
                    {"name": "delivery_address", "type": "TEXT", "nullable": True},
                    {"name": "estimated_delivery_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "delivered_at", "type": "TIMESTAMP", "nullable": True},
                    {"name": "is_paid", "type": "BOOLEAN", "default": False},
                ],
            },
            "reservations": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "restaurant_id", "type": "INT", "foreign_key": "restaurants.id"},
                    {"name": "customer_name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "party_size", "type": "INT", "nullable": False},
                    {"name": "reserved_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["confirmed", "cancelled", "no_show", "completed"]},
                    {"name": "customer_phone", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "customer_email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "special_requests", "type": "TEXT", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
            },
            "reviews": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "restaurant_id", "type": "INT", "foreign_key": "restaurants.id"},
                    {"name": "rating", "type": "INT", "nullable": False},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "optional_cols": [
                    {"name": "reviewer_name", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "title", "type": "VARCHAR(200)", "nullable": True},
                    {"name": "body", "type": "TEXT", "nullable": True},
                    {"name": "food_rating", "type": "INT", "nullable": True},
                    {"name": "service_rating", "type": "INT", "nullable": True},
                    {"name": "ambiance_rating", "type": "INT", "nullable": True},
                    {"name": "is_verified", "type": "BOOLEAN", "default": False},
                ],
            },
            "delivery_drivers": {
                "base_cols": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                    {"name": "phone", "type": "VARCHAR(20)", "nullable": False},
                    {"name": "is_available", "type": "BOOLEAN", "default": True},
                ],
                "optional_cols": [
                    {"name": "vehicle_type", "type": "VARCHAR(20)", "enum_values": ["bicycle", "motorcycle", "car", "van"]},
                    {"name": "rating", "type": "FLOAT", "nullable": True},
                    {"name": "total_deliveries", "type": "INT", "default": 0},
                    {"name": "joined_at", "type": "DATE", "nullable": False},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                ],
            },
        },
    },
}

SCHEMAS_PER_DOMAIN = 20  # 20 schemas × 10 domains = 200


def select_tables(domain_config, rng):
    """Select 3-8 tables for a schema, always including tables with FKs' targets."""
    all_tables = list(domain_config["tables"].keys())
    num_tables = rng.randint(3, min(8, len(all_tables)))
    selected = rng.sample(all_tables, num_tables)

    # Ensure FK targets are included
    changed = True
    while changed:
        changed = False
        for tname in list(selected):
            tconf = domain_config["tables"][tname]
            for col in tconf["base_cols"] + tconf["optional_cols"]:
                if "foreign_key" in col:
                    ref_table = col["foreign_key"].split(".")[0]
                    if ref_table not in selected and ref_table in all_tables:
                        selected.append(ref_table)
                        changed = True

    return selected


def build_table(table_name, table_config, rng):
    """Build a table with base columns + random subset of optional columns."""
    cols = list(table_config["base_cols"])
    optional = table_config["optional_cols"]
    if optional:
        # Ensure at least enough optional cols to reach 4 total columns minimum
        min_optional = max(1, 4 - len(cols))
        min_optional = min(min_optional, len(optional))
        num_optional = rng.randint(min_optional, len(optional))
        cols.extend(rng.sample(optional, num_optional))
    return {"name": table_name, "columns": cols}


def extract_relationships(tables):
    """Extract foreign key relationships from the table set."""
    relationships = []
    for table in tables:
        for col in table["columns"]:
            if "foreign_key" in col:
                relationships.append({
                    "from": f"{table['name']}.{col['name']}",
                    "to": col["foreign_key"],
                    "type": "many_to_one",
                })
    return relationships


def validate_schema(schema):
    """Validate schema integrity."""
    table_names = {t["name"] for t in schema["tables"]}

    for table in schema["tables"]:
        col_names = [c["name"] for c in table["columns"]]
        if len(col_names) != len(set(col_names)):
            return False, f"Duplicate column names in {table['name']}"

    for rel in schema.get("relationships", []):
        ref_table = rel["to"].split(".")[0]
        if ref_table not in table_names:
            return False, f"FK references missing table: {rel['to']}"

    for table in schema["tables"]:
        if len(table["columns"]) < 4:
            return False, f"Table {table['name']} has fewer than 4 columns"

    return True, "OK"


def generate_schemas():
    """Generate all 200 schemas."""
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    rng = random.Random(42)

    for domain, domain_config in DOMAINS.items():
        for i in range(1, SCHEMAS_PER_DOMAIN + 1):
            schema_id = f"{domain}_{i:03d}"
            selected_table_names = select_tables(domain_config, rng)
            tables = []
            for tname in selected_table_names:
                tables.append(build_table(tname, domain_config["tables"][tname], rng))

            # Filter out FK columns whose target table is not selected
            for table in tables:
                table["columns"] = [
                    col for col in table["columns"]
                    if "foreign_key" not in col
                    or col["foreign_key"].split(".")[0] in {t["name"] for t in tables}
                ]

            relationships = extract_relationships(tables)

            schema = {
                "schema_id": schema_id,
                "domain": domain,
                "tables": tables,
                "relationships": relationships,
            }

            valid, msg = validate_schema(schema)
            if not valid:
                print(f"WARNING: {schema_id} failed validation: {msg}")
                continue

            out_path = SCHEMAS_DIR / f"{schema_id}.json"
            with open(out_path, "w") as f:
                json.dump(schema, f, indent=2)

            manifest.append({"schema_id": schema_id, "domain": domain})

    # Write manifest
    manifest_path = SCHEMAS_DIR / "_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump({"total": len(manifest), "schemas": manifest}, f, indent=2)

    print(f"Generated {len(manifest)} schemas across {len(DOMAINS)} domains")
    print(f"Schemas saved to {SCHEMAS_DIR}")

    # Print domain distribution
    from collections import Counter
    domain_counts = Counter(s["domain"] for s in manifest)
    for domain, count in sorted(domain_counts.items()):
        print(f"  {domain}: {count}")

    return manifest


if __name__ == "__main__":
    generate_schemas()
