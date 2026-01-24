CREATE DATABASE streaming_data;

CREATE SCHEMA IF NOT EXISTS ingested;

CREATE TABLE streaming_data.ingested.tick_kraken (
    price_significand INTEGER NOT NULL,
    price_exp INTEGER NOT NULL,
    ts timestamp NOT NULL
);

CREATE TABLE streaming_data.ingested.historical_kraken (
    high_significand INTEGER NOT NULL,
    low_significand INTEGER NOT NULL,
    d date NOT NULL
);