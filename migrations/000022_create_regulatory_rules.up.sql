-- Effective-dated regulatory rules. Every rule row belongs to one immutable dataset version; the
-- dataset carries jurisdiction, source, version and licence, so rule rows do not repeat them.
-- Effective dates are calendar dates: effective_from inclusive, effective_to exclusive, NULL = open
-- (never 'infinity', so open-ended has one spelling).
-- Within one dataset a rule key may not have overlapping validity (EXCLUDE ... WITH &&); the same
-- key in another dataset version is allowed, since each load repeats the rules it still contains.
-- This migration owns btree_gist: its down drops it.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE DOMAIN regulatory_date AS DATE
    CONSTRAINT chk_regulatory_date_finite CHECK (isfinite(VALUE));
-- Digits only, 2-10 in pairs: chapter, heading, subheading, national lines.
CREATE DOMAIN hs_code AS VARCHAR(10)
    CONSTRAINT chk_hs_code_format CHECK (VALUE ~ '^([0-9]{2}){1,5}$');
CREATE DOMAIN country_code AS VARCHAR(2)
    CONSTRAINT chk_country_code_format CHECK (VALUE ~ '^[A-Z]{2}$');
-- '*' = every country; a rule for one country is keyed separately, never folded into '*'.
CREATE DOMAIN country_code_or_any AS VARCHAR(2)
    CONSTRAINT chk_country_code_or_any_format CHECK (VALUE = '*' OR VALUE ~ '^[A-Z]{2}$');

CREATE TABLE regulatory_datasets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    jurisdiction country_code NOT NULL,
    category VARCHAR(32) NOT NULL CHECK (category IN (
        'TARIFF', 'IMPORT_RESTRICTION', 'PERMIT', 'SANCTIONS', 'EXPORT_CONTROL', 'PREFERENTIAL_AGREEMENT'
    )),
    source VARCHAR(64) NOT NULL CHECK (btrim(source) <> ''),
    version VARCHAR(64) NOT NULL CHECK (btrim(version) <> ''),
    fetched_at TIMESTAMPTZ NOT NULL,
    content_sha256 BYTEA NOT NULL CHECK (octet_length(content_sha256) = 32),
    licence VARCHAR(128) NOT NULL CHECK (btrim(licence) <> ''),
    attribution TEXT NOT NULL CHECK (btrim(attribution) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_regulatory_datasets_version UNIQUE (jurisdiction, category, source, version),
    -- Target of the rule tables' (dataset_id, category) foreign keys.
    CONSTRAINT uq_regulatory_datasets_id_category UNIQUE (id, category)
);

-- measure_type: MFN is the base rate; ADDITIONAL_DUTY stacks on it (US Chapter 99, NG levies);
-- PREFERENTIAL replaces it for a qualifying origin. measure_code names the programme or agreement.
-- UNSUPPORTED_MEASURE keeps a published rate the engine cannot evaluate, so evaluation can HOLD.
-- Rates are unconstrained NUMERIC: a typmod would round a published rate silently.
CREATE TABLE tariff_rates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    category VARCHAR(32) NOT NULL DEFAULT 'TARIFF' CHECK (category = 'TARIFF'),
    hs_code hs_code NOT NULL,
    origin_country country_code_or_any NOT NULL,
    measure_type VARCHAR(16) NOT NULL CHECK (measure_type IN ('MFN', 'ADDITIONAL_DUTY', 'PREFERENTIAL')),
    measure_code VARCHAR(32) CHECK (measure_code ~ '^[A-Z0-9_]+$'),
    rate_type VARCHAR(24) NOT NULL CHECK (rate_type IN ('AD_VALOREM', 'SPECIFIC', 'COMPOUND', 'UNSUPPORTED_MEASURE')),
    ad_valorem_percent NUMERIC CHECK (ad_valorem_percent >= 0),
    specific_amount NUMERIC CHECK (specific_amount >= 0),
    specific_currency VARCHAR(3) CHECK (specific_currency ~ '^[A-Z]{3}$'),
    specific_unit VARCHAR(16) CHECK (btrim(specific_unit) <> ''),
    rate_expression TEXT NOT NULL CHECK (btrim(rate_expression) <> ''),
    effective_from regulatory_date NOT NULL,
    effective_to regulatory_date,
    source_reference TEXT NOT NULL CHECK (btrim(source_reference) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_tariff_rates_dataset FOREIGN KEY (dataset_id, category)
        REFERENCES regulatory_datasets (id, category) ON DELETE RESTRICT,
    CONSTRAINT chk_tariff_rates_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT chk_tariff_rates_measure_code CHECK ((measure_type = 'MFN') = (measure_code IS NULL)),
    CONSTRAINT chk_tariff_rates_preferential_origin CHECK (measure_type <> 'PREFERENTIAL' OR origin_country <> '*'),
    CONSTRAINT chk_tariff_rates_rate_components CHECK (
        (rate_type IN ('AD_VALOREM', 'COMPOUND')) = (ad_valorem_percent IS NOT NULL)
        AND (rate_type IN ('SPECIFIC', 'COMPOUND')) = (specific_amount IS NOT NULL)
        AND (specific_amount IS NULL) = (specific_currency IS NULL)
        AND (specific_amount IS NULL) = (specific_unit IS NULL)
    ),
    CONSTRAINT ex_tariff_rates_no_overlap EXCLUDE USING gist (
        dataset_id WITH =, hs_code WITH =, origin_country WITH =, measure_type WITH =,
        (COALESCE(measure_code, '')) WITH =, daterange(effective_from, effective_to) WITH &&
    )
);

-- Prohibited imports for the dataset's jurisdiction. Goods allowed only with a permit are
-- permit_requirements rows, not restrictions.
CREATE TABLE import_restrictions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    category VARCHAR(32) NOT NULL DEFAULT 'IMPORT_RESTRICTION' CHECK (category = 'IMPORT_RESTRICTION'),
    hs_code hs_code NOT NULL,
    origin_country country_code_or_any NOT NULL,
    description TEXT NOT NULL CHECK (btrim(description) <> ''),
    effective_from regulatory_date NOT NULL,
    effective_to regulatory_date,
    source_reference TEXT NOT NULL CHECK (btrim(source_reference) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_import_restrictions_dataset FOREIGN KEY (dataset_id, category)
        REFERENCES regulatory_datasets (id, category) ON DELETE RESTRICT,
    CONSTRAINT chk_import_restrictions_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT ex_import_restrictions_no_overlap EXCLUDE USING gist (
        dataset_id WITH =, hs_code WITH =, origin_country WITH =,
        daterange(effective_from, effective_to) WITH &&
    )
);

CREATE TABLE permit_requirements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    category VARCHAR(32) NOT NULL DEFAULT 'PERMIT' CHECK (category = 'PERMIT'),
    hs_code hs_code NOT NULL,
    origin_country country_code_or_any NOT NULL,
    permit_code VARCHAR(64) NOT NULL CHECK (btrim(permit_code) <> ''),
    issuing_agency VARCHAR(255) NOT NULL CHECK (btrim(issuing_agency) <> ''),
    document_type VARCHAR(128) NOT NULL CHECK (btrim(document_type) <> ''),
    effective_from regulatory_date NOT NULL,
    effective_to regulatory_date,
    source_reference TEXT NOT NULL CHECK (btrim(source_reference) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_permit_requirements_dataset FOREIGN KEY (dataset_id, category)
        REFERENCES regulatory_datasets (id, category) ON DELETE RESTRICT,
    CONSTRAINT chk_permit_requirements_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT ex_permit_requirements_no_overlap EXCLUDE USING gist (
        dataset_id WITH =, hs_code WITH =, origin_country WITH =, permit_code WITH =,
        daterange(effective_from, effective_to) WITH &&
    )
);

-- Designated parties on a sanctions list. list_entry_id is the list's own identifier for the
-- designation; effective_from is the listing date, effective_to the delisting date.
CREATE TABLE sanctions_list (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    category VARCHAR(32) NOT NULL DEFAULT 'SANCTIONS' CHECK (category = 'SANCTIONS'),
    list_entry_id VARCHAR(128) NOT NULL CHECK (btrim(list_entry_id) <> ''),
    entity_type VARCHAR(16) NOT NULL CHECK (entity_type IN ('INDIVIDUAL', 'ENTITY', 'VESSEL', 'AIRCRAFT')),
    primary_name VARCHAR(512) NOT NULL CHECK (btrim(primary_name) <> ''),
    program VARCHAR(128) NOT NULL CHECK (btrim(program) <> ''),
    effective_from regulatory_date NOT NULL,
    effective_to regulatory_date,
    source_reference TEXT NOT NULL CHECK (btrim(source_reference) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_sanctions_list_dataset FOREIGN KEY (dataset_id, category)
        REFERENCES regulatory_datasets (id, category) ON DELETE RESTRICT,
    CONSTRAINT chk_sanctions_list_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT ex_sanctions_list_no_overlap EXCLUDE USING gist (
        dataset_id WITH =, list_entry_id WITH =, daterange(effective_from, effective_to) WITH &&
    )
);

-- Export licence requirements of the dataset's (exporting) jurisdiction.
CREATE TABLE export_controls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    category VARCHAR(32) NOT NULL DEFAULT 'EXPORT_CONTROL' CHECK (category = 'EXPORT_CONTROL'),
    hs_code hs_code NOT NULL,
    destination_country country_code_or_any NOT NULL,
    control_code VARCHAR(32) NOT NULL CHECK (btrim(control_code) <> ''),
    licence_type VARCHAR(64) NOT NULL CHECK (btrim(licence_type) <> ''),
    effective_from regulatory_date NOT NULL,
    effective_to regulatory_date,
    source_reference TEXT NOT NULL CHECK (btrim(source_reference) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_export_controls_dataset FOREIGN KEY (dataset_id, category)
        REFERENCES regulatory_datasets (id, category) ON DELETE RESTRICT,
    CONSTRAINT chk_export_controls_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT ex_export_controls_no_overlap EXCLUDE USING gist (
        dataset_id WITH =, hs_code WITH =, destination_country WITH =, control_code WITH =,
        daterange(effective_from, effective_to) WITH &&
    )
);

-- Partner countries of a trade agreement in force for the dataset's (importing) jurisdiction.
-- The preferential rates themselves are tariff_rates rows with measure_type PREFERENTIAL and
-- measure_code = agreement_code; the two live in different datasets, so no foreign key links them.
CREATE TABLE preferential_agreements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    category VARCHAR(32) NOT NULL DEFAULT 'PREFERENTIAL_AGREEMENT' CHECK (category = 'PREFERENTIAL_AGREEMENT'),
    agreement_code VARCHAR(32) NOT NULL CHECK (agreement_code ~ '^[A-Z0-9_]+$'),
    partner_country country_code NOT NULL,
    proof_of_origin VARCHAR(255) NOT NULL CHECK (btrim(proof_of_origin) <> ''),
    effective_from regulatory_date NOT NULL,
    effective_to regulatory_date,
    source_reference TEXT NOT NULL CHECK (btrim(source_reference) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_preferential_agreements_dataset FOREIGN KEY (dataset_id, category)
        REFERENCES regulatory_datasets (id, category) ON DELETE RESTRICT,
    CONSTRAINT chk_preferential_agreements_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT ex_preferential_agreements_no_overlap EXCLUDE USING gist (
        dataset_id WITH =, agreement_code WITH =, partner_country WITH =,
        daterange(effective_from, effective_to) WITH &&
    )
);
