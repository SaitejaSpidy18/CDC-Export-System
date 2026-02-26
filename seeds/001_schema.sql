-- 001_schema.sql
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);
-- Index on updated_at for CDC queries
CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);
CREATE TABLE IF NOT EXISTS watermarks (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(255) NOT NULL UNIQUE,
    last_exported_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);