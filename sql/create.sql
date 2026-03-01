CREATE SCHEMA IF NOT EXISTS security_master;

CREATE TYPE security_master.contract_type AS ENUM ('put', 'call');
CREATE TYPE security_master.exercise_style AS ENUM ('american', 'european');

CREATE TABLE IF NOT EXISTS security_master.venue (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS security_master.asset (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR NOT NULL,
    venue BIGINT NOT NULL REFERENCES security_master.venue(id),
    instrument_type VARCHAR NOT NULL,

    UNIQUE(ticker, venue, instrument_type)
);

CREATE TABLE IF NOT EXISTS security_master.options (
    security_id BIGINT PRIMARY KEY REFERENCES security_master.asset(id),
    contract_type security_master.contract_type NOT NULL,
    exercise_style security_master.exercise_style NOT NULL,
    strike_price NUMERIC(10,2) NOT NULL,
    expiration_date DATE NOT NULL,
    shares_per_contract INT NOT NULL
);

CREATE TABLE IF NOT EXISTS security_master.rates (
    security_id BIGINT PRIMARY KEY REFERENCES security_master.asset(id)
);

CREATE TABLE IF NOT EXISTS security_master.equities (
    security_id BIGINT PRIMARY KEY REFERENCES security_master.asset(id)
);

CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.massive (
    security_id BIGINT NOT NULL REFERENCES security_master.asset(id), 
    price NUMERIC(10,2) NOT NULL,
    time TIMESTAMP NOT NULL,

    UNIQUE (security_id, time)
);

CREATE TABLE IF NOT EXISTS market_data.fed (
    security_id BIGINT NOT NULL REFERENCES security_master.asset(id),
    effective_date DATE NOT NULL,
    rate NUMERIC(10,4) NOT NULL,

    UNIQUE (security_id, effective_date)
);