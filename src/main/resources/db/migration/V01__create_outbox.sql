CREATE TABLE IF NOT EXISTS outbox (
    id BigSerial,
    msg Utf8,
    status Utf8 NOT NULL,
    PRIMARY KEY (id)
);