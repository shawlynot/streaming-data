CREATE SCHEMA IF NOT EXISTS security_master;

CREATE TYPE security_master.contract_type AS ENUM ('put', 'call');
CREATE TYPE security_master.exercise_style AS ENUM ('american', 'european');

CREATE TABLE IF NOT EXISTS security_master.options (
    ticker VARCHAR(50) NOT NULL PRIMARY KEY,
    contract_type security_master.contract_type NOT NULL,
    exercise_style security_master.exercise_style NOT NULL,
    strike_price NUMERIC(10,2) NOT NULL,
    expiration_date DATE NOT NULL,
    shares_per_contract INT NOT NULL
);

CREATE SCHEMA IF NOT EXISTS ingested;

CREATE table if not exists ingested.option_massive (
    ticker VARCHAR(50) NOT NULL REFERENCES security_master.options(ticker), 
    open NUMERIC(10,2) NOT NULL,
    close NUMERIC(10,2) NOT NULL,
    _date date NOT NULL,
    UNIQUE (ticker, _date)
);

CREATE TABLE IF NOT EXISTS ingested.spot_massive (
    open NUMERIC(10,2) NOT NULL,
    close NUMERIC(10,2) NOT NULL,
    _date date NOT NULL UNIQUE
);
