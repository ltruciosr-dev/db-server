CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS demographics (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    age INTEGER,
    gender VARCHAR(50),
    income_level VARCHAR(50),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS onboarding (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    step INTEGER,
    status VARCHAR(50),
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_status (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    status VARCHAR(50),
    last_active_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    account_type VARCHAR(50),
    balance DECIMAL(15,2),
    currency VARCHAR(10),
    activated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS card_info (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id),
    card_number VARCHAR(20),
    expiration_date DATE,
    status VARCHAR(50)
);