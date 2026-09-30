DROP TRIGGER IF EXISTS trg_preferential_agreements_frozen ON preferential_agreements;
DROP TRIGGER IF EXISTS trg_export_controls_frozen ON export_controls;
DROP TRIGGER IF EXISTS trg_sanctions_list_frozen ON sanctions_list;
DROP TRIGGER IF EXISTS trg_permit_requirements_frozen ON permit_requirements;
DROP TRIGGER IF EXISTS trg_import_restrictions_frozen ON import_restrictions;
DROP TRIGGER IF EXISTS trg_tariff_rates_frozen ON tariff_rates;
DROP FUNCTION IF EXISTS regulatory_rules_frozen();
DROP INDEX IF EXISTS uq_regulatory_datasets_active;
ALTER TABLE regulatory_datasets
    DROP CONSTRAINT IF EXISTS chk_regulatory_datasets_review_before_activation,
    DROP CONSTRAINT IF EXISTS chk_regulatory_datasets_status_columns,
    DROP CONSTRAINT IF EXISTS chk_regulatory_datasets_reviewer_not_loader,
    DROP CONSTRAINT IF EXISTS chk_regulatory_datasets_review_complete,
    DROP COLUMN IF EXISTS rejected_reason,
    DROP COLUMN IF EXISTS superseded_at,
    DROP COLUMN IF EXISTS activated_at,
    DROP COLUMN IF EXISTS review_note,
    DROP COLUMN IF EXISTS reviewed_at,
    DROP COLUMN IF EXISTS reviewed_by,
    DROP COLUMN IF EXISTS requires_review,
    DROP COLUMN IF EXISTS loaded_by,
    DROP COLUMN IF EXISTS status;
