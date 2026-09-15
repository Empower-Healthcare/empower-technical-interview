-- Handy queries for the interview. Each has a Makefile wrapper, in order:
--   make sql-shipment SHIPMENT_ID=shp_...   make sql-dispatch-counts   make sql-pending
--   make sql-processed                      make sql-event EVENT_ID=evt_...
-- Anything else: make psql (interactive) or make sql Q="select ...".
-- All data is fictional.

-- One shipment end to end: stored price, outbox state, dispatch state.
-- Replace the shipment_id.
SELECT s.shipment_id,
       s.weight_grams,
       s.amount_cents,
       s.currency,
       o.event_id,
       o.published_at IS NOT NULL AS published,
       o.attempts,
       d.dispatch_id,
       d.dispatched_at
FROM shipments s
LEFT JOIN outbox_events o USING (shipment_id)
LEFT JOIN dispatches    d USING (shipment_id)
WHERE s.shipment_id = 'shp_...';

-- Dispatch count per shipment. Must never exceed 1 (UNIQUE (shipment_id)).
SELECT shipment_id, count(*) AS dispatches
FROM dispatches
GROUP BY shipment_id
ORDER BY dispatches DESC, shipment_id
LIMIT 20;

-- Pending outbox events (grows while Kafka is down, drains after recovery).
SELECT event_id, shipment_id, created_at, attempts, last_error
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at;

-- Which event IDs the consumer has already seen.
SELECT event_id, processed_at
FROM processed_events
ORDER BY processed_at DESC
LIMIT 20;

-- The payload that was published for an event.
SELECT jsonb_pretty(payload)
FROM outbox_events
WHERE event_id = 'evt_...';
