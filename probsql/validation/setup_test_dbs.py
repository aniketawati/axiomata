"""
Test Database Setup — Creates 5 SQLite databases with realistic test data.

Usage: python phase4_validation/setup_test_dbs.py

Creates:
1. ecommerce.db — 10K customers, 50K orders, 100K line items
2. saas.db — 5K users, 3K subscriptions, 20K usage events
3. healthcare.db — 8K patients, 15K appointments, 10K prescriptions
4. hr.db — 2K employees, 20 departments, 5K leave requests
5. education.db — 3K students, 200 courses, 10K enrollments
"""

import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_DIR = Path(__file__).parent / "test_databases"
SEED = 42


def random_date(rng, start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 1, 15)
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, delta))


def random_timestamp(rng, start_year=2022, end_year=2025):
    d = random_date(rng, start_year, end_year)
    return d.replace(hour=rng.randint(0, 23), minute=rng.randint(0, 59), second=rng.randint(0, 59))


def nullable(rng, value, prob=0.15):
    return None if rng.random() < prob else value


def create_ecommerce_db(rng):
    """Create ecommerce.db with customers, orders, products, categories, order_items, reviews."""
    db_path = DB_DIR / "ecommerce.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            phone TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP,
            is_verified BOOLEAN DEFAULT 0,
            status TEXT CHECK(status IN ('active','inactive','suspended')),
            lifetime_value DECIMAL(10,2) DEFAULT 0,
            city TEXT,
            country TEXT
        );
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INTEGER,
            is_active BOOLEAN DEFAULT 1
        );
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            category_id INTEGER REFERENCES categories(id),
            created_at TIMESTAMP NOT NULL,
            description TEXT,
            stock_quantity INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT 1,
            rating FLOAT,
            review_count INTEGER DEFAULT 0,
            brand TEXT
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(id),
            total_amount DECIMAL(10,2) NOT NULL,
            status TEXT CHECK(status IN ('pending','processing','shipped','delivered','cancelled','refunded')),
            created_at TIMESTAMP NOT NULL,
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP,
            payment_method TEXT,
            is_gift BOOLEAN DEFAULT 0
        );
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER REFERENCES orders(id),
            product_id INTEGER REFERENCES products(id),
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL
        );
        CREATE TABLE reviews (
            id INTEGER PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            customer_id INTEGER REFERENCES customers(id),
            rating INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            title TEXT,
            body TEXT,
            is_verified_purchase BOOLEAN DEFAULT 0,
            status TEXT CHECK(status IN ('pending','approved','rejected'))
        );
    """)

    # Categories
    cats = ["Electronics", "Clothing", "Home & Garden", "Books", "Sports", "Toys", "Food", "Beauty", "Auto", "Office"]
    for i, cat in enumerate(cats, 1):
        c.execute("INSERT INTO categories VALUES (?,?,?,?)", (i, cat, None, 1))

    # Products (1000)
    brands = ["Apple", "Samsung", "Nike", "Sony", "LG", "Dell", "HP", "Adidas", "Puma", None]
    for i in range(1, 1001):
        c.execute("INSERT INTO products VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (i, f"Product {i}", round(rng.uniform(5, 500), 2), rng.randint(1, 10),
                   random_timestamp(rng).isoformat(), nullable(rng, f"Description for product {i}"),
                   rng.randint(0, 1000), rng.random() > 0.1,
                   nullable(rng, round(rng.uniform(1, 5), 1), 0.3), rng.randint(0, 500),
                   rng.choice(brands)))

    # Customers (10K)
    statuses = ["active"] * 7 + ["inactive"] * 2 + ["suspended"]
    countries = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "BR", "IN", "MX"]
    cities = ["New York", "London", "Toronto", "Berlin", "Paris", "Sydney", "Tokyo", "Sao Paulo", "Mumbai", "Mexico City"]
    for i in range(1, 10001):
        c.execute("INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (i, f"user{i}@example.com", f"User {i}", nullable(rng, f"+1-555-{i:04d}"),
                   random_timestamp(rng).isoformat(), nullable(rng, random_timestamp(rng).isoformat()),
                   rng.random() > 0.3, rng.choice(statuses),
                   round(rng.uniform(0, 5000), 2),
                   nullable(rng, rng.choice(cities), 0.2), rng.choice(countries)))

    # Orders (50K)
    order_statuses = ["pending", "processing", "shipped", "delivered", "delivered", "delivered", "cancelled", "refunded"]
    payments = ["credit_card", "debit_card", "paypal", "bank_transfer"]
    for i in range(1, 50001):
        status = rng.choice(order_statuses)
        created = random_timestamp(rng)
        shipped = nullable(rng, (created + timedelta(days=rng.randint(1, 5))).isoformat()) if status in ("shipped", "delivered") else None
        delivered = nullable(rng, (created + timedelta(days=rng.randint(3, 14))).isoformat()) if status == "delivered" else None
        c.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 10000), round(rng.uniform(10, 1000), 2), status,
                   created.isoformat(), shipped, delivered,
                   rng.choice(payments), rng.random() > 0.95))

    # Order items (100K)
    for i in range(1, 100001):
        qty = rng.randint(1, 5)
        price = round(rng.uniform(5, 200), 2)
        c.execute("INSERT INTO order_items VALUES (?,?,?,?,?)",
                  (i, rng.randint(1, 50000), rng.randint(1, 1000), qty, price))

    # Reviews (20K)
    review_statuses = ["approved"] * 8 + ["pending"] + ["rejected"]
    for i in range(1, 20001):
        c.execute("INSERT INTO reviews VALUES (?,?,?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 1000), rng.randint(1, 10000), rng.randint(1, 5),
                   random_timestamp(rng).isoformat(),
                   nullable(rng, f"Review title {i}", 0.3),
                   nullable(rng, f"Review body for product...", 0.2),
                   rng.random() > 0.4, rng.choice(review_statuses)))

    conn.commit()
    conn.close()
    print(f"Created {db_path}")


def create_saas_db(rng):
    """Create saas.db."""
    db_path = DB_DIR / "saas.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE plans (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price_monthly DECIMAL(10,2) NOT NULL,
            tier TEXT CHECK(tier IN ('free','starter','pro','enterprise')),
            max_users INTEGER,
            is_active BOOLEAN DEFAULT 1
        );
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            plan_id INTEGER REFERENCES plans(id),
            is_active BOOLEAN DEFAULT 1,
            member_count INTEGER DEFAULT 1
        );
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            role TEXT CHECK(role IN ('admin','member','viewer','owner')),
            last_login_at TIMESTAMP,
            team_id INTEGER REFERENCES teams(id),
            is_verified BOOLEAN DEFAULT 0
        );
        CREATE TABLE subscriptions (
            id INTEGER PRIMARY KEY,
            team_id INTEGER REFERENCES teams(id),
            plan_id INTEGER REFERENCES plans(id),
            status TEXT CHECK(status IN ('active','cancelled','past_due','trialing','expired')),
            started_at TIMESTAMP NOT NULL,
            cancelled_at TIMESTAMP,
            monthly_amount DECIMAL(10,2),
            is_annual BOOLEAN DEFAULT 0
        );
        CREATE TABLE usage_logs (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            action TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            duration_ms INTEGER
        );
    """)

    # Plans
    plans = [(1, "Free", 0, "free", 3, 1), (2, "Starter", 29, "starter", 10, 1),
             (3, "Pro", 99, "pro", 50, 1), (4, "Enterprise", 299, "enterprise", None, 1)]
    for p in plans:
        c.execute("INSERT INTO plans VALUES (?,?,?,?,?,?)", p)

    # Teams (500)
    for i in range(1, 501):
        c.execute("INSERT INTO teams VALUES (?,?,?,?,?,?)",
                  (i, f"Team {i}", random_timestamp(rng).isoformat(),
                   rng.randint(1, 4), rng.random() > 0.1, rng.randint(1, 50)))

    # Users (5K)
    roles = ["admin", "member", "member", "member", "viewer", "owner"]
    for i in range(1, 5001):
        c.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?,?)",
                  (i, f"user{i}@saas.com", f"SaaS User {i}",
                   random_timestamp(rng).isoformat(), rng.random() > 0.1,
                   rng.choice(roles), nullable(rng, random_timestamp(rng).isoformat(), 0.2),
                   rng.randint(1, 500), rng.random() > 0.3))

    # Subscriptions (3K)
    sub_statuses = ["active"] * 6 + ["cancelled", "past_due", "trialing", "expired"]
    for i in range(1, 3001):
        status = rng.choice(sub_statuses)
        plan_id = rng.randint(1, 4)
        c.execute("INSERT INTO subscriptions VALUES (?,?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 500), plan_id, status,
                   random_timestamp(rng).isoformat(),
                   nullable(rng, random_timestamp(rng).isoformat()) if status == "cancelled" else None,
                   plans[plan_id - 1][2], rng.random() > 0.7))

    # Usage logs (20K)
    actions = ["login", "view_dashboard", "create_project", "invite_member", "export_data",
               "update_settings", "api_call", "file_upload", "search", "billing_update"]
    for i in range(1, 20001):
        c.execute("INSERT INTO usage_logs VALUES (?,?,?,?,?)",
                  (i, rng.randint(1, 5000), rng.choice(actions),
                   random_timestamp(rng).isoformat(), rng.randint(10, 5000)))

    conn.commit()
    conn.close()
    print(f"Created {db_path}")


def create_healthcare_db(rng):
    """Create healthcare.db."""
    db_path = DB_DIR / "healthcare.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE patients (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            date_of_birth DATE NOT NULL,
            gender TEXT CHECK(gender IN ('male','female','other')),
            created_at TIMESTAMP NOT NULL,
            email TEXT,
            phone TEXT,
            blood_type TEXT,
            is_active BOOLEAN DEFAULT 1
        );
        CREATE TABLE doctors (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            specialization TEXT NOT NULL,
            is_available BOOLEAN DEFAULT 1,
            years_experience INTEGER,
            rating FLOAT,
            department TEXT
        );
        CREATE TABLE appointments (
            id INTEGER PRIMARY KEY,
            patient_id INTEGER REFERENCES patients(id),
            doctor_id INTEGER REFERENCES doctors(id),
            scheduled_at TIMESTAMP NOT NULL,
            status TEXT CHECK(status IN ('scheduled','completed','cancelled','no_show')),
            type TEXT CHECK(type IN ('checkup','follow_up','emergency','consultation','procedure')),
            duration_minutes INTEGER DEFAULT 30,
            fee DECIMAL(10,2),
            notes TEXT
        );
        CREATE TABLE prescriptions (
            id INTEGER PRIMARY KEY,
            patient_id INTEGER REFERENCES patients(id),
            doctor_id INTEGER REFERENCES doctors(id),
            medication TEXT NOT NULL,
            prescribed_at TIMESTAMP NOT NULL,
            dosage TEXT,
            is_active BOOLEAN DEFAULT 1,
            refills_remaining INTEGER DEFAULT 0
        );
        CREATE TABLE diagnoses (
            id INTEGER PRIMARY KEY,
            patient_id INTEGER REFERENCES patients(id),
            doctor_id INTEGER REFERENCES doctors(id),
            condition TEXT NOT NULL,
            diagnosed_at TIMESTAMP NOT NULL,
            severity TEXT CHECK(severity IN ('mild','moderate','severe','critical')),
            is_chronic BOOLEAN DEFAULT 0
        );
    """)

    # Doctors (100)
    specializations = ["Cardiology", "Dermatology", "Neurology", "Orthopedics", "Pediatrics",
                       "Psychiatry", "Surgery", "Internal Medicine", "Radiology", "Emergency"]
    departments = ["ER", "ICU", "Outpatient", "Surgery", "Pediatrics"]
    for i in range(1, 101):
        c.execute("INSERT INTO doctors VALUES (?,?,?,?,?,?,?)",
                  (i, f"Dr. {chr(65 + i % 26)}{i}", rng.choice(specializations),
                   rng.random() > 0.1, rng.randint(1, 35),
                   round(rng.uniform(3, 5), 1), rng.choice(departments)))

    # Patients (8K)
    genders = ["male", "female", "other"]
    blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    for i in range(1, 8001):
        dob = random_date(rng, 1940, 2010)
        c.execute("INSERT INTO patients VALUES (?,?,?,?,?,?,?,?,?)",
                  (i, f"Patient {i}", dob.strftime("%Y-%m-%d"),
                   rng.choices(genders, weights=[48, 48, 4])[0],
                   random_timestamp(rng).isoformat(),
                   nullable(rng, f"patient{i}@email.com"),
                   nullable(rng, f"+1-555-{i:04d}"),
                   nullable(rng, rng.choice(blood_types), 0.2), rng.random() > 0.05))

    # Appointments (15K)
    appt_statuses = ["completed"] * 6 + ["scheduled"] * 2 + ["cancelled", "no_show"]
    appt_types = ["checkup", "follow_up", "follow_up", "consultation", "emergency", "procedure"]
    for i in range(1, 15001):
        c.execute("INSERT INTO appointments VALUES (?,?,?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 8000), rng.randint(1, 100),
                   random_timestamp(rng).isoformat(), rng.choice(appt_statuses),
                   rng.choice(appt_types), rng.choice([15, 30, 30, 45, 60]),
                   round(rng.uniform(50, 500), 2),
                   nullable(rng, "Follow up needed", 0.6)))

    # Prescriptions (10K)
    meds = ["Amoxicillin", "Lisinopril", "Metformin", "Atorvastatin", "Omeprazole",
            "Ibuprofen", "Acetaminophen", "Prednisone", "Albuterol", "Gabapentin"]
    for i in range(1, 10001):
        c.execute("INSERT INTO prescriptions VALUES (?,?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 8000), rng.randint(1, 100), rng.choice(meds),
                   random_timestamp(rng).isoformat(),
                   nullable(rng, f"{rng.randint(1, 4)}x daily"),
                   rng.random() > 0.3, rng.randint(0, 5)))

    # Diagnoses (12K)
    conditions = ["Hypertension", "Diabetes Type 2", "Asthma", "Depression", "Anxiety",
                  "Back Pain", "Arthritis", "Migraine", "Allergies", "Infection"]
    severities = ["mild", "mild", "moderate", "moderate", "severe", "critical"]
    for i in range(1, 12001):
        c.execute("INSERT INTO diagnoses VALUES (?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 8000), rng.randint(1, 100), rng.choice(conditions),
                   random_timestamp(rng).isoformat(), rng.choice(severities),
                   rng.random() > 0.7))

    conn.commit()
    conn.close()
    print(f"Created {db_path}")


def create_hr_db(rng):
    """Create hr.db."""
    db_path = DB_DIR / "hr.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            budget DECIMAL(15,2),
            is_active BOOLEAN DEFAULT 1,
            location TEXT
        );
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            department_id INTEGER REFERENCES departments(id),
            hired_at DATE NOT NULL,
            status TEXT CHECK(status IN ('active','on_leave','terminated','probation')),
            title TEXT,
            salary DECIMAL(12,2),
            is_remote BOOLEAN DEFAULT 0,
            performance_rating FLOAT
        );
        CREATE TABLE leave_requests (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER REFERENCES employees(id),
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            status TEXT CHECK(status IN ('pending','approved','rejected','cancelled')),
            type TEXT CHECK(type IN ('vacation','sick','personal','maternity','paternity','bereavement')),
            reason TEXT,
            days_count INTEGER
        );
        CREATE TABLE performance_reviews (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER REFERENCES employees(id),
            reviewer_id INTEGER REFERENCES employees(id),
            rating FLOAT NOT NULL,
            review_date DATE NOT NULL,
            comments TEXT,
            period TEXT CHECK(period IN ('Q1','Q2','Q3','Q4','annual'))
        );
    """)

    # Departments (20)
    depts = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Legal",
             "Product", "Design", "Operations", "Support", "Data Science",
             "Security", "QA", "DevOps", "Research", "Customer Success",
             "Compliance", "IT", "Procurement", "Facilities"]
    locations = ["New York", "San Francisco", "London", "Berlin", "Remote"]
    for i, dept in enumerate(depts, 1):
        c.execute("INSERT INTO departments VALUES (?,?,?,?,?)",
                  (i, dept, round(rng.uniform(100000, 5000000), 2), 1, rng.choice(locations)))

    # Employees (2K)
    emp_statuses = ["active"] * 8 + ["on_leave", "terminated", "probation"]
    titles = ["Software Engineer", "Senior Engineer", "Product Manager", "Designer",
              "Data Analyst", "Marketing Manager", "Sales Rep", "HR Specialist",
              "Team Lead", "Director", "VP", "Intern"]
    for i in range(1, 2001):
        c.execute("INSERT INTO employees VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (i, f"Employee {i}", f"emp{i}@company.com",
                   rng.randint(1, 20), random_date(rng, 2015, 2025).strftime("%Y-%m-%d"),
                   rng.choice(emp_statuses), rng.choice(titles),
                   round(rng.uniform(40000, 250000), 2), rng.random() > 0.6,
                   nullable(rng, round(rng.uniform(1, 5), 1), 0.2)))

    # Leave requests (5K)
    leave_statuses = ["approved"] * 5 + ["pending"] * 2 + ["rejected", "cancelled"]
    leave_types = ["vacation"] * 4 + ["sick"] * 3 + ["personal", "maternity", "bereavement"]
    for i in range(1, 5001):
        start = random_date(rng, 2023, 2025)
        days = rng.randint(1, 14)
        c.execute("INSERT INTO leave_requests VALUES (?,?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 2000), start.strftime("%Y-%m-%d"),
                   (start + timedelta(days=days)).strftime("%Y-%m-%d"),
                   rng.choice(leave_statuses), rng.choice(leave_types),
                   nullable(rng, "Personal reasons", 0.4), days))

    # Performance reviews (4K)
    periods = ["Q1", "Q2", "Q3", "Q4", "annual"]
    for i in range(1, 4001):
        c.execute("INSERT INTO performance_reviews VALUES (?,?,?,?,?,?,?)",
                  (i, rng.randint(1, 2000), rng.randint(1, 2000),
                   round(rng.uniform(1, 5), 1), random_date(rng, 2023, 2025).strftime("%Y-%m-%d"),
                   nullable(rng, "Good performance", 0.3), rng.choice(periods)))

    conn.commit()
    conn.close()
    print(f"Created {db_path}")


def create_education_db(rng):
    """Create education.db."""
    db_path = DB_DIR / "education.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE instructors (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            department TEXT NOT NULL,
            title TEXT,
            is_tenured BOOLEAN DEFAULT 0,
            rating FLOAT
        );
        CREATE TABLE students (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            enrolled_at DATE NOT NULL,
            status TEXT CHECK(status IN ('active','graduated','suspended','withdrawn')),
            gpa FLOAT,
            major TEXT,
            is_international BOOLEAN DEFAULT 0,
            scholarship_amount DECIMAL(10,2) DEFAULT 0
        );
        CREATE TABLE courses (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            credits INTEGER NOT NULL,
            instructor_id INTEGER REFERENCES instructors(id),
            department TEXT,
            is_active BOOLEAN DEFAULT 1,
            max_enrollment INTEGER
        );
        CREATE TABLE enrollments (
            id INTEGER PRIMARY KEY,
            student_id INTEGER REFERENCES students(id),
            course_id INTEGER REFERENCES courses(id),
            enrolled_at TIMESTAMP NOT NULL,
            status TEXT CHECK(status IN ('enrolled','dropped','completed','failed')),
            grade TEXT
        );
        CREATE TABLE assignments (
            id INTEGER PRIMARY KEY,
            course_id INTEGER REFERENCES courses(id),
            title TEXT NOT NULL,
            due_date TIMESTAMP NOT NULL,
            max_score FLOAT DEFAULT 100,
            type TEXT CHECK(type IN ('homework','quiz','midterm','final','project','lab'))
        );
    """)

    # Instructors (50)
    instructor_depts = ["Computer Science", "Mathematics", "Physics", "English", "History",
                        "Biology", "Chemistry", "Economics", "Psychology", "Engineering"]
    instructor_titles = ["professor", "associate_professor", "assistant_professor", "lecturer", "adjunct"]
    for i in range(1, 51):
        c.execute("INSERT INTO instructors VALUES (?,?,?,?,?,?,?)",
                  (i, f"Prof. {chr(65 + i % 26)}{i}", f"prof{i}@university.edu",
                   rng.choice(instructor_depts), rng.choice(instructor_titles),
                   rng.random() > 0.6, round(rng.uniform(2.5, 5), 1)))

    # Students (3K)
    student_statuses = ["active"] * 7 + ["graduated"] * 2 + ["suspended", "withdrawn"]
    majors = ["Computer Science", "Mathematics", "Physics", "English", "Business",
              "Biology", "Psychology", "Engineering", "Economics", "History"]
    for i in range(1, 3001):
        c.execute("INSERT INTO students VALUES (?,?,?,?,?,?,?,?,?)",
                  (i, f"Student {i}", f"student{i}@university.edu",
                   random_date(rng, 2019, 2024).strftime("%Y-%m-%d"),
                   rng.choice(student_statuses),
                   nullable(rng, round(rng.uniform(1.5, 4.0), 2), 0.1),
                   rng.choice(majors), rng.random() > 0.8,
                   round(rng.uniform(0, 20000), 2) if rng.random() > 0.6 else 0))

    # Courses (200)
    for i in range(1, 201):
        dept = rng.choice(instructor_depts)
        c.execute("INSERT INTO courses VALUES (?,?,?,?,?,?,?,?)",
                  (i, f"{dept} {100 + i}", f"{dept[:3].upper()}{100 + i}",
                   rng.choice([3, 3, 3, 4]), rng.randint(1, 50),
                   dept, rng.random() > 0.1, rng.randint(20, 200)))

    # Enrollments (10K)
    enroll_statuses = ["completed"] * 5 + ["enrolled"] * 3 + ["dropped", "failed"]
    grades = ["A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F", None]
    for i in range(1, 10001):
        status = rng.choice(enroll_statuses)
        grade = rng.choice(grades) if status in ("completed", "failed") else None
        c.execute("INSERT INTO enrollments VALUES (?,?,?,?,?,?)",
                  (i, rng.randint(1, 3000), rng.randint(1, 200),
                   random_timestamp(rng).isoformat(), status, grade))

    # Assignments (1000)
    assign_types = ["homework", "homework", "quiz", "midterm", "final", "project", "lab"]
    for i in range(1, 1001):
        c.execute("INSERT INTO assignments VALUES (?,?,?,?,?,?)",
                  (i, rng.randint(1, 200), f"Assignment {i}",
                   random_timestamp(rng).isoformat(),
                   rng.choice([10, 20, 50, 100, 100]),
                   rng.choice(assign_types)))

    conn.commit()
    conn.close()
    print(f"Created {db_path}")


def main():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)

    create_ecommerce_db(rng)
    create_saas_db(rng)
    create_healthcare_db(rng)
    create_hr_db(rng)
    create_education_db(rng)

    print(f"\nAll test databases created in {DB_DIR}")
    for db_file in sorted(DB_DIR.glob("*.db")):
        size_kb = db_file.stat().st_size / 1024
        print(f"  {db_file.name}: {size_kb:.0f} KB")


if __name__ == "__main__":
    main()
