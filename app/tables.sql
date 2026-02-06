CREATE SCHEMA IF NOT EXISTS ingested;

CREATE table if not exists ingested.option_massive (
    open NUMERIC(10,2) NOT NULL,
    close NUMERIC(10,2) NOT NULL,
    _date date NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested.spot_massive (
    open NUMERIC(10,2) NOT NULL,
    close NUMERIC(10,2) NOT NULL,
    _date date NOT NULL
);

CREATE SCHEMA IF NOT EXISTS security_master;

CREATE TYPE contract_type AS ENUM ('put', 'call');
CREATE TYPE exercise_style AS ENUM ('american', 'european');

CREATE TABLE IF NOT EXISTS security_master.options (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    contract_type contract_type NOT NULL,
    exercise_style exercise_style NOT NULL,
    strike_price NUMERIC(10,2) NOT NULL,
    expiration_date DATE NOT NULL,
    shares_per_contract INT NOT NULL
)