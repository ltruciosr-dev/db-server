CREATE TABLE IF NOT EXISTS transactions_mastercard (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    amount DECIMAL(15,2),
    merchant VARCHAR(255),
    account_type VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS transactions_paypal (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    amount DECIMAL(15,2),
    merchant VARCHAR(255),
    account_type VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS transactions_internal (
    id SERIAL PRIMARY KEY,
    sender_id INTEGER,
    receiver_id INTEGER,
    amount DECIMAL(15,2),
    account_type VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50)
);

-- THE NEXT TABLE IS CREATED AS A VIEW
-- CREATE TABLE IF NOT EXISTS operations (
--     id SERIAL PRIMARY KEY,
--     user_id INTEGER,
--     amount DECIMAL(15,2),
--     transaction_type VARCHAR(50),
--     line_id INTEGER,
--     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );