-- name: CreateRegulatoryDataset :exec
INSERT INTO regulatory_datasets (
    id, jurisdiction, category, source, version, fetched_at, content_sha256, licence, attribution,
    status, loaded_by, requires_review
) VALUES (
    @id, @jurisdiction, @category, @source, @version, @fetched_at, @content_sha256, @licence, @attribution,
    @status, @loaded_by, @requires_review
);

-- name: GetRegulatoryDatasetForUpdate :one
SELECT id, jurisdiction, category, source, version, fetched_at, content_sha256, licence, attribution, created_at,
       status, loaded_by, requires_review, reviewed_by, reviewed_at, review_note, activated_at,
       superseded_at, rejected_reason
FROM regulatory_datasets
WHERE id = @id
FOR UPDATE;

-- name: GetActiveRegulatoryDatasetForUpdate :one
SELECT id, jurisdiction, category, source, version, fetched_at, content_sha256, licence, attribution, created_at,
       status, loaded_by, requires_review, reviewed_by, reviewed_at, review_note, activated_at,
       superseded_at, rejected_reason
FROM regulatory_datasets
WHERE jurisdiction = @jurisdiction AND category = @category AND source = @source AND status = 'ACTIVE'
FOR UPDATE;

-- name: UpdateRegulatoryDatasetLifecycle :execrows
UPDATE regulatory_datasets
SET status = @status,
    reviewed_by = @reviewed_by,
    reviewed_at = @reviewed_at,
    review_note = @review_note,
    activated_at = @activated_at,
    superseded_at = @superseded_at,
    rejected_reason = @rejected_reason
WHERE id = @id AND status = @expected_status;

-- A dataset is in force at T from its activation until its supersession.
-- name: ListRegulatoryDatasetsActiveAt :many
SELECT id, jurisdiction, category, source, version, fetched_at, content_sha256, licence, attribution, created_at,
       status, loaded_by, requires_review, reviewed_by, reviewed_at, review_note, activated_at,
       superseded_at, rejected_reason
FROM regulatory_datasets
WHERE jurisdiction = @jurisdiction AND category = @category
  AND activated_at <= @at::timestamptz
  AND (superseded_at IS NULL OR superseded_at > @at::timestamptz)
ORDER BY source, id;

-- Parallel unnest in the select list zips the arrays row by row; the repository passes equal lengths.
-- name: InsertImportRestrictions :exec
INSERT INTO import_restrictions (dataset_id, hs_code, origin_country, description, effective_from, effective_to,
                                 source_reference)
SELECT @dataset_id, unnest(@hs_codes::text[]), unnest(@origin_countries::text[]), unnest(@descriptions::text[]),
       unnest(@effective_froms::date[]), unnest(@effective_tos::date[]), unnest(@source_references::text[]);

-- name: InsertPermitRequirements :exec
INSERT INTO permit_requirements (dataset_id, hs_code, origin_country, permit_code, issuing_agency, document_type,
                                 effective_from, effective_to, source_reference)
SELECT @dataset_id, unnest(@hs_codes::text[]), unnest(@origin_countries::text[]), unnest(@permit_codes::text[]),
       unnest(@issuing_agencies::text[]), unnest(@document_types::text[]), unnest(@effective_froms::date[]),
       unnest(@effective_tos::date[]), unnest(@source_references::text[]);
