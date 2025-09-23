CREATE TABLE IF NOT EXISTS reward_events (
    id IDENTITY PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    points INT NOT NULL,
    tx_amount DECIMAL(10,2) NOT NULL,
    tx_time TIMESTAMP NOT NULL
);
