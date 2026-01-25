CREATE SCHEMA IF NOT EXISTS ingested;

CREATE table if not exists ingested.tick_kraken (
    price NUMERIC(10,2) NOT NULL,
    ts timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested.historical_kraken (
    open NUMERIC(10,2) NOT NULL,
    close NUMERIC(10,2) NOT NULL,
    _date date NOT NULL
);
