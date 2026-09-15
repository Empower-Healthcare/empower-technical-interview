-- ParcelLab schema, version 1.
--
-- Applied by `cmd/migrate` (see db/embed.go and internal/storage/migrate.go).
-- Every table here is fictional interview material; there is no production data.

-- A shipment is written together with its outbox event in one transaction
-- (see storage.Store.CreateShipment).
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id   TEXT        PRIMARY KEY,
    weight_grams  INTEGER     NOT NULL CHECK (weight_grams BETWEEN 1 AND 30000),
    amount_cents  BIGINT      NOT NULL CHECK (amount_cents >= 0),
    currency      CHAR(3)     NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox. The relay polls rows where published_at IS NULL,
-- publishes them to Kafka, waits for the broker acknowledgment and then sets
-- published_at. event_id is stable across retries and replays, which is what
-- lets the consumer deduplicate.
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id      TEXT        PRIMARY KEY,
    shipment_id   TEXT        NOT NULL REFERENCES shipments (shipment_id),
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    attempts      INTEGER     NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

-- Consumer-side deduplication. Inserting an event_id that already exists fails
-- with a unique violation, which the consumer treats as "already processed".
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      TEXT        PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The fictional dispatch produced by the consumer. The UNIQUE constraint on
-- shipment_id is a second line of defence: even if two different event IDs
-- referred to the same shipment, only one dispatch could exist.
CREATE TABLE IF NOT EXISTS dispatches (
    dispatch_id    TEXT        PRIMARY KEY,
    shipment_id    TEXT        NOT NULL UNIQUE REFERENCES shipments (shipment_id),
    event_id       TEXT        NOT NULL REFERENCES processed_events (event_id),
    amount_cents   BIGINT      NOT NULL CHECK (amount_cents >= 0),
    currency       CHAR(3)     NOT NULL,
    dispatched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
