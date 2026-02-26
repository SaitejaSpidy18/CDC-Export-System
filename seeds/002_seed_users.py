import os
import sys
from datetime import datetime, timedelta, timezone
import random
import psycopg2
from faker import Faker
def get_connection():
    # Use env vars set in docker-compose (Postgres container)
    db_user = os.environ.get("POSTGRES_USER", "user")
    db_password = os.environ.get("POSTGRES_PASSWORD", "password")
    db_name = os.environ.get("POSTGRES_DB", "mydatabase")
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=5432,
    )
    return conn
def main():
    fake = Faker()
    target_count = 100_000
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()
    # Idempotency: if we already have enough users, do nothing
    cur.execute("SELECT COUNT(*) FROM users;")
    (current_count,) = cur.fetchone()
    if current_count >= target_count:
        print(f"Users already seeded ({current_count} rows), skipping.")
        cur.close()
        conn.close()
        return
    remaining = target_count - current_count
    print(f"Seeding {remaining} users...")
    now = datetime.now(timezone.utc)
    min_days = 0
    max_days = 30
    batch_size = 1000
    inserted = 0
    insert_sql = '''
        INSERT INTO users (name, email, created_at, updated_at, is_deleted)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (email) DO NOTHING;
    '''
    while inserted < remaining:
        batch = []
        for _ in range(min(batch_size, remaining - inserted)):
            # Random created_at in last 30 days
            delta_days = random.randint(min_days, max_days)
            created_at = now - timedelta(days=delta_days, hours=random.randint(0, 23), minutes=random.randint(0, 59))
            # updated_at >= created_at, sometimes equal
            if random.random() < 0.5:
                updated_at = created_at
            else:
                updated_at = created_at + timedelta(
                    days=random.randint(0, 3),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                if updated_at > now:
                    updated_at = now
            name = fake.name()
            email = fake.unique.email()
            # 1–5% soft-deleted
            is_deleted = random.random() < 0.03
            batch.append((name, email, created_at, updated_at, is_deleted))
        cur.executemany(insert_sql, batch)
        inserted += len(batch)
        conn.commit()
        print(f"Inserted {inserted}/{remaining} users...")
    cur.close()
    conn.close()
    print("Seeding complete.")
if __name__ == "__main__":
    main()