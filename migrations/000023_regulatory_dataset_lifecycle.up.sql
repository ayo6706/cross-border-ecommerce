-- Dataset lifecycle: LOADED -> ACTIVE -> SUPERSEDED, or LOADED -> REJECTED. A dataset affects
-- decisions at T when activated_at <= T < superseded_at. No column has a default: the domain
-- entity decides every value, and a writer that forgets one fails here.
ALTER TABLE regulatory_datasets
    ADD COLUMN status VARCHAR(16) NOT NULL CHECK (status IN ('LOADED', 'ACTIVE', 'SUPERSEDED', 'REJECTED')),
    ADD COLUMN loaded_by VARCHAR(128) NOT NULL CHECK (btrim(loaded_by) <> ''),
    ADD COLUMN requires_review BOOLEAN NOT NULL,
    ADD COLUMN reviewed_by VARCHAR(128),
    ADD COLUMN reviewed_at TIMESTAMPTZ,
    ADD COLUMN review_note TEXT,
    ADD COLUMN activated_at TIMESTAMPTZ,
    ADD COLUMN superseded_at TIMESTAMPTZ,
    ADD COLUMN rejected_reason TEXT,
    ADD CONSTRAINT chk_regulatory_datasets_review_complete CHECK (
        (reviewed_by IS NULL AND reviewed_at IS NULL AND review_note IS NULL)
        OR (btrim(reviewed_by) <> '' AND reviewed_at IS NOT NULL AND btrim(review_note) <> '')
    ),
    ADD CONSTRAINT chk_regulatory_datasets_reviewer_not_loader CHECK (
        lower(btrim(reviewed_by)) <> lower(btrim(loaded_by))
    ),
    ADD CONSTRAINT chk_regulatory_datasets_status_columns CHECK (
        CASE status
            WHEN 'LOADED' THEN activated_at IS NULL AND superseded_at IS NULL AND rejected_reason IS NULL
            WHEN 'ACTIVE' THEN activated_at IS NOT NULL AND superseded_at IS NULL AND rejected_reason IS NULL
            WHEN 'SUPERSEDED' THEN activated_at IS NOT NULL AND superseded_at >= activated_at
                AND rejected_reason IS NULL
            WHEN 'REJECTED' THEN activated_at IS NULL AND superseded_at IS NULL AND btrim(rejected_reason) <> ''
        END
    ),
    ADD CONSTRAINT chk_regulatory_datasets_review_before_activation CHECK (
        NOT requires_review OR activated_at IS NULL OR reviewed_at IS NOT NULL
    );

-- One ACTIVE version per key; a concurrent second activation fails with 23505.
CREATE UNIQUE INDEX uq_regulatory_datasets_active
    ON regulatory_datasets (jurisdiction, category, source) WHERE status = 'ACTIVE';

-- Rules may change only while their dataset is LOADED: once activated (or rejected) a version is
-- what decisions were, or will be, made against. FOR SHARE waits for a concurrent activation.
CREATE FUNCTION regulatory_rules_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    dataset_status VARCHAR(16);
BEGIN
    FOR dataset_status IN
        SELECT status FROM regulatory_datasets
        WHERE id IN (
            CASE WHEN TG_OP <> 'INSERT' THEN OLD.dataset_id END,
            CASE WHEN TG_OP <> 'DELETE' THEN NEW.dataset_id END
        )
        FOR SHARE
    LOOP
        IF dataset_status <> 'LOADED' THEN
            RAISE EXCEPTION 'regulatory dataset is %, its rules can no longer change', dataset_status
                USING ERRCODE = 'object_not_in_prerequisite_state', TABLE = TG_TABLE_NAME;
        END IF;
    END LOOP;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER trg_tariff_rates_frozen BEFORE INSERT OR UPDATE OR DELETE ON tariff_rates
    FOR EACH ROW EXECUTE FUNCTION regulatory_rules_frozen();
CREATE TRIGGER trg_import_restrictions_frozen BEFORE INSERT OR UPDATE OR DELETE ON import_restrictions
    FOR EACH ROW EXECUTE FUNCTION regulatory_rules_frozen();
CREATE TRIGGER trg_permit_requirements_frozen BEFORE INSERT OR UPDATE OR DELETE ON permit_requirements
    FOR EACH ROW EXECUTE FUNCTION regulatory_rules_frozen();
CREATE TRIGGER trg_sanctions_list_frozen BEFORE INSERT OR UPDATE OR DELETE ON sanctions_list
    FOR EACH ROW EXECUTE FUNCTION regulatory_rules_frozen();
CREATE TRIGGER trg_export_controls_frozen BEFORE INSERT OR UPDATE OR DELETE ON export_controls
    FOR EACH ROW EXECUTE FUNCTION regulatory_rules_frozen();
CREATE TRIGGER trg_preferential_agreements_frozen BEFORE INSERT OR UPDATE OR DELETE ON preferential_agreements
    FOR EACH ROW EXECUTE FUNCTION regulatory_rules_frozen();
