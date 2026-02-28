CREATE SCHEMA IF NOT EXISTS security_master;

CREATE TYPE security_master.contract_type AS ENUM ('put', 'call');
CREATE TYPE security_master.exercise_style AS ENUM ('american', 'european');

CREATE TABLE IF NOT EXISTS security_master.asset (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(50) NOT NULL,
    instrument_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS security_master.options (
    security_id BIGINT PRIMARY KEY REFERENCES security_master.asset(id),
    ticker VARCHAR(50) NOT NULL,
    contract_type security_master.contract_type NOT NULL,
    exercise_style security_master.exercise_style NOT NULL,
    strike_price NUMERIC(10,2) NOT NULL,
    expiration_date DATE NOT NULL,
    shares_per_contract INT NOT NULL
);

CREATE TABLE IF NOT EXISTS security_master.equities (
    security_id BIGINT PRIMARY KEY REFERENCES security_master.asset(id),
    ticker VARCHAR(50) NOT NULL
);

CREATE SCHEMA IF NOT EXISTS market_data;

CREATE table if NOT EXISTS market_data.massive (
    security_id BIGINT NOT NULL REFERENCES security_master.asset(id), 
    price NUMERIC(10,2) NOT NULL,
    time timestamp NOT NULL,
    UNIQUE (security_id, time)
);
