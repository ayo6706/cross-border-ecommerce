-- Representative dataset for the query-plan suite (query_plan_test.go). Sized so the planner
-- prefers an index wherever one serves the query, and small enough to seed in about a minute:
--   ingestion_runs            20,001 (20,000 COMPLETED across 100 bulk sources + 1 RUNNING)
--   ingestion_run_processing  20,000 COMPLETED (the RUNNING run is queued when the suite completes it)
--   raw_records              210,000 (200,000 in 20 bulk runs + 10,000 in the run to process)
--   products / product_versions / product_sources / product_changes  100,000 each
--   outbox_events            201,000 (200,000 PROCESSED + 1,000 PENDING)
-- The RUNNING run belongs to src-perf. Its records use ext-95000..ext-104999; product_sources
-- holds ext-0..ext-99999 for src-perf, so half the records change a product and half are new.

INSERT INTO sources (id, name, type)
SELECT 'src-bulk-' || g, 'Bulk source ' || g, 'FEED' FROM generate_series(1, 100) g;

INSERT INTO sources (id, name, type, config)
VALUES ('src-perf', 'Query plan source', 'API', '{
    "base_url": "https://supplier.invalid/v1",
    "field_mapping": {
        "name_path": "title",
        "description_path": "body",
        "brand_path": "vendor",
        "origin_country_path": "country_code"
    }
}');

INSERT INTO ingestion_runs (source_id, status, started_at, completed_at, created_at, updated_at)
SELECT 'src-bulk-' || (g % 100 + 1), 'COMPLETED',
       now() - (g || ' minutes')::interval, now() - (g || ' minutes')::interval,
       now() - (g || ' minutes')::interval, now() - (g || ' minutes')::interval
FROM generate_series(1, 20000) g;

INSERT INTO ingestion_run_processing (run_id, status, started_at, completed_at)
SELECT id, 'COMPLETED', completed_at, completed_at FROM ingestion_runs;

CREATE TEMP TABLE seed_runs AS
SELECT id, row_number() OVER (ORDER BY created_at) AS n
FROM ingestion_runs WHERE source_id = 'src-bulk-1' ORDER BY created_at LIMIT 20;

INSERT INTO raw_records (source_id, external_product_id, payload, ingestion_run_id, received_at)
SELECT 'src-bulk-1', 'ext-' || (g % 100000), '{"title": "Bulk"}'::jsonb, r.id,
       now() - (g || ' seconds')::interval
FROM generate_series(1, 200000) g
JOIN seed_runs r ON r.n = g % 20 + 1;

INSERT INTO products (canonical_name, created_at, updated_at)
SELECT 'Product ' || g, now() - (g || ' seconds')::interval, now() - (g || ' seconds')::interval
FROM generate_series(0, 99999) g;

CREATE TEMP TABLE seed_products AS
SELECT id, row_number() OVER (ORDER BY created_at DESC, id) - 1 AS n FROM products;

INSERT INTO product_versions (product_id, version_number, fingerprint, canonical_name)
SELECT id, 1, 'seed:' || md5(id::text), 'Product' FROM seed_products;

UPDATE products p SET current_version_id = pv.id, current_fingerprint = pv.fingerprint
FROM product_versions pv WHERE pv.product_id = p.id;

INSERT INTO product_sources (product_id, source_id, external_product_id, last_received_at)
SELECT id, 'src-perf', 'ext-' || n, now() - interval '1 day' FROM seed_products;

INSERT INTO product_changes (product_id, to_version_id, change_type)
SELECT product_id, id, 'NEW' FROM product_versions;

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, processed_at,
                           created_at, available_at)
SELECT 'product', g::text, 'product.changed', '{}'::jsonb, 'PROCESSED', now(),
       now() - (g || ' seconds')::interval, now() - (g || ' seconds')::interval
FROM generate_series(1, 200000) g;

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
SELECT 'product', 'pending-' || g, 'product.changed', '{}'::jsonb FROM generate_series(1, 1000) g;

INSERT INTO ingestion_runs (source_id, status, started_at)
VALUES ('src-perf', 'RUNNING', now());

INSERT INTO raw_records (source_id, external_product_id, payload, ingestion_run_id, received_at)
SELECT 'src-perf', 'ext-' || g,
       jsonb_build_object('title', 'Perf product ' || g, 'body', 'Body', 'vendor', 'Vendor',
                          'country_code', 'US'),
       (SELECT id FROM ingestion_runs WHERE source_id = 'src-perf'), now()
FROM generate_series(95000, 104999) g;
