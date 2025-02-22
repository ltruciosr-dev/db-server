CREATE TABLE IF NOT EXISTS campaigns (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    goal DECIMAL(15,2),
    cashback_percentage DECIMAL(5,2),
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS user_campaigns (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    campaign_id INTEGER REFERENCES campaigns(id),
    merchant_list TEXT[], 
    start_date DATE,
    end_date DATE
);