-- ENG-017 evidence: GET /v1/products keyset pages at depth, against OFFSET, on 1,000,000 products.
--
-- RUN ONLY AGAINST A THROWAWAY DATABASE. It truncates the catalogue tables.
--   go run ./cmd/migrate up                              # DATABASE_URL = the throwaway database
--   psql "$DATABASE_URL" -f scripts/explain/products_keyset.sql > keyset.txt
--
-- Dataset: 1,000,000 products in 2,000 ingestion-like batches of 500 that share one created_at,
-- so most keyset positions sit inside a tie and the id tie-break is exercised.
-- Queries keep the WHERE / ORDER BY / LIMIT of ListProductsFirstPage / ListProductsAfterCursor
-- (products.sql) with the repository's limit + 1 (501 for a 500-item page).

\set ON_ERROR_STOP on
\timing off

TRUNCATE product_changes, product_sources, product_versions, products CASCADE;

INSERT INTO products (canonical_name, description, created_at, updated_at)
SELECT 'Product ' || g, repeat('d', 200),
       timestamptz '2026-09-01' + ((g / 500) || ' seconds')::interval,
       timestamptz '2026-09-01' + ((g / 500) || ' seconds')::interval
FROM generate_series(1, 1000000) g;

VACUUM ANALYZE products;

SELECT count(*) AS products, count(DISTINCT created_at) AS distinct_created_at FROM products;
SELECT pg_size_pretty(pg_relation_size('products')) AS heap,
       pg_size_pretty(pg_relation_size('idx_products_created_at_id')) AS keyset_index;

\echo '=== 1. First page: ListProductsFirstPage, limit 501 ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, canonical_name, description, brand, origin_country, status,
       current_version_id, current_fingerprint, created_at, updated_at
FROM products
ORDER BY created_at DESC, id DESC
LIMIT 501;

\echo '=== 2. Depth 500,000: ListProductsAfterCursor, limit 501 ==='
SELECT created_at AS cursor_created_at, id AS cursor_id
FROM products ORDER BY created_at DESC, id DESC OFFSET 499999 LIMIT 1 \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, canonical_name, description, brand, origin_country, status,
       current_version_id, current_fingerprint, created_at, updated_at
FROM products
WHERE (created_at, id) < (:'cursor_created_at'::timestamptz, :'cursor_id'::uuid)
ORDER BY created_at DESC, id DESC
LIMIT 501;

\echo '=== 3. Depth 999,500 (last page): ListProductsAfterCursor, limit 501 ==='
SELECT created_at AS cursor_created_at, id AS cursor_id
FROM products ORDER BY created_at DESC, id DESC OFFSET 999499 LIMIT 1 \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, canonical_name, description, brand, origin_country, status,
       current_version_id, current_fingerprint, created_at, updated_at
FROM products
WHERE (created_at, id) < (:'cursor_created_at'::timestamptz, :'cursor_id'::uuid)
ORDER BY created_at DESC, id DESC
LIMIT 501;

\echo '=== 4. For comparison, not used by the API: OFFSET 500000, limit 501 ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, canonical_name, description, brand, origin_country, status,
       current_version_id, current_fingerprint, created_at, updated_at
FROM products
ORDER BY created_at DESC, id DESC
OFFSET 500000 LIMIT 501;
