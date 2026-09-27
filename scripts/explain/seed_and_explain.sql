-- ENG-016 index evidence: seeds a representative dataset and prints EXPLAIN (ANALYZE, BUFFERS) for
-- every query whose index ADR 0010 keeps.
--
-- RUN ONLY AGAINST A THROWAWAY DATABASE. It truncates the catalogue, ingestion and outbox tables.
--   go run ./cmd/migrate up                              # DATABASE_URL = the throwaway database
--   psql "$DATABASE_URL" -f scripts/explain/seed_and_explain.sql > explain.txt
--
-- Dataset (sized to run on a 16 GB laptop in a few minutes, not to the 20M/day target):
--   outbox_events      1,000,000 PROCESSED + 50 PENDING
--   raw_records        1,000,000 across 20 runs of one source (50,000 per run)
--   products           200,000 with one version and one source identity each
--   product_changes    200,000
--   ingestion_runs     2,000 across 100 sources
-- Each EXPLAIN keeps the query's WHERE / ORDER BY / LIMIT from internal/infrastructure/postgres/queries
-- with literal parameters; select lists are shortened where they do not change the access path.

\set ON_ERROR_STOP on
\timing off

TRUNCATE product_changes, product_sources, product_versions, products, raw_records,
         ingestion_run_processing, ingestion_runs, outbox_events, sources CASCADE;

INSERT INTO sources (id, name, type)
SELECT 'src-' || g, 'Source ' || g, 'FEED' FROM generate_series(1, 100) g;

INSERT INTO ingestion_runs (source_id, status, created_at)
SELECT 'src-' || (g % 100 + 1), 'COMPLETED', now() - (g || ' minutes')::interval
FROM generate_series(1, 2000) g;

CREATE TEMP TABLE seed_runs AS
SELECT id, row_number() OVER (ORDER BY created_at) AS n
FROM ingestion_runs WHERE source_id = 'src-1' ORDER BY created_at LIMIT 20;

INSERT INTO raw_records (source_id, external_product_id, payload, ingestion_run_id, received_at)
SELECT 'src-1', 'ext-' || (g % 200000), '{"name":"x"}'::jsonb,
       (SELECT id FROM seed_runs WHERE n = g % 20 + 1),
       now() - (g || ' seconds')::interval
FROM generate_series(1, 1000000) g;

INSERT INTO products (canonical_name, created_at)
SELECT 'Product ' || g, now() - (g || ' seconds')::interval FROM generate_series(1, 200000) g;

CREATE TEMP TABLE seed_products AS
SELECT id, row_number() OVER (ORDER BY created_at DESC, id) - 1 AS n FROM products;

INSERT INTO product_versions (product_id, version_number, fingerprint, canonical_name)
SELECT id, 1, 'v1:' || md5(id::text), 'Product' FROM seed_products;

UPDATE products p SET current_version_id = pv.id FROM product_versions pv WHERE pv.product_id = p.id;

INSERT INTO product_sources (product_id, source_id, external_product_id, last_received_at)
SELECT id, 'src-1', 'ext-' || n, now() FROM seed_products;

INSERT INTO product_changes (product_id, to_version_id, change_type)
SELECT product_id, id, 'NEW' FROM product_versions;

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, processed_at, created_at, available_at)
SELECT 'product', g::text, 'product.changed', '{}'::jsonb, 'PROCESSED', now(),
       now() - (g || ' seconds')::interval, now() - (g || ' seconds')::interval
FROM generate_series(1, 1000000) g;

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
SELECT 'product', g::text, 'product.changed', '{}'::jsonb FROM generate_series(1, 50) g;

INSERT INTO ingestion_run_processing (run_id, status)
SELECT id, 'COMPLETED' FROM ingestion_runs OFFSET 5;
INSERT INTO ingestion_run_processing (run_id)
SELECT id FROM ingestion_runs LIMIT 5;

ANALYZE;

\echo '=== 1. ClaimOutboxBatch candidates (idx_outbox_claimable) ==='
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
WITH candidate AS (
    SELECT id FROM outbox_events
    WHERE status = 'PENDING' AND available_at <= NOW()
    ORDER BY available_at, id
    LIMIT 100
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events o
SET claim_token = gen_random_uuid(), available_at = NOW() + interval '30 seconds'
FROM candidate WHERE o.id = candidate.id;
ROLLBACK;

\echo '=== 2. ListRawRecordsByRunIDKeysetAfterCursor (idx_raw_records_run_keyset) ==='
SELECT id AS run_id FROM seed_runs WHERE n = 7 \gset
SELECT id AS cursor_id FROM raw_records WHERE ingestion_run_id = :'run_id' ORDER BY id OFFSET 25000 LIMIT 1 \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, source_id, external_product_id, payload, received_at
FROM raw_records
WHERE ingestion_run_id = :'run_id' AND id > :'cursor_id'::uuid
ORDER BY id ASC
LIMIT 500;

\echo '=== 3. GetLatestRawRecordBySourceAndExternalID (idx_raw_records_source_external_received) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, payload, received_at
FROM raw_records
WHERE source_id = 'src-1' AND external_product_id = 'ext-4242'
ORDER BY received_at DESC, id DESC
LIMIT 1;

\echo '=== 4. ListProductsAfterCursor (idx_products_created_at_id) ==='
SELECT created_at AS cursor_created_at, id AS cursor_id FROM products ORDER BY created_at DESC, id DESC OFFSET 100000 LIMIT 1 \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, canonical_name, created_at
FROM products
WHERE (created_at, id) < (:'cursor_created_at'::timestamptz, :'cursor_id'::uuid)
ORDER BY created_at DESC, id DESC
LIMIT 50;

\echo '=== 5. GetProductWithSourceByIdentities, 500 identities (uq_product_sources_source_external) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT ps.id, p.canonical_name, pv.fingerprint
FROM product_sources ps
JOIN products p ON ps.product_id = p.id
LEFT JOIN product_versions pv ON p.current_version_id = pv.id
WHERE (ps.source_id, ps.external_product_id) IN (
    SELECT unnest(array_fill('src-1'::varchar, ARRAY[500])),
           unnest(ARRAY(SELECT ('ext-' || g * 397 % 200000)::varchar FROM generate_series(1, 500) g))
);

\echo '=== 6. ListProductVersions (uq_product_versions_product_version; replaces idx_product_versions_product_id) ==='
SELECT id AS product_id FROM products ORDER BY created_at OFFSET 12345 LIMIT 1 \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, version_number, fingerprint
FROM product_versions
WHERE product_id = :'product_id'
ORDER BY version_number DESC;

\echo '=== 7. ListProductChangesByProductID (idx_product_changes_product_detected) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, change_type, detected_at
FROM product_changes
WHERE product_id = :'product_id'
ORDER BY detected_at DESC, id DESC
LIMIT 20;

\echo '=== 8. GetLatestIngestionRunBySource (idx_ingestion_runs_source_created) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, status, created_at
FROM ingestion_runs
WHERE source_id = 'src-42'
ORDER BY created_at DESC, id DESC
LIMIT 1;

\echo '=== 9. ClaimNextRunProcessing candidate (idx_ingestion_run_processing_claim) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT run_id FROM ingestion_run_processing
WHERE status = 'PENDING' OR (status = 'RUNNING' AND lease_expires_at < NOW())
ORDER BY created_at ASC
LIMIT 1;

\echo '=== Index sizes ==='
SELECT indexrelname AS index, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes ORDER BY pg_relation_size(indexrelid) DESC;
