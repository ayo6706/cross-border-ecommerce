-- Regulatory test fixtures for NG, US, UK and EU. Never load this into a deployed database:
-- rates, parties and dates are shaped like the real sources but are not current law, and the
-- sanctioned parties are fictional. Deployed data comes from the regulatory loaders.

INSERT INTO regulatory_datasets (id, jurisdiction, category, source, version, fetched_at, content_sha256, licence, attribution, status, loaded_by, requires_review) VALUES
    ('00000000-0000-4000-8000-000000000001', 'NG', 'TARIFF', 'ng_cet', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('ng_cet fixture'), 'Public sector information', 'Nigeria Customs Service, ECOWAS CET (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000002', 'US', 'TARIFF', 'usitc_hts', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('usitc_hts fixture'), 'Public domain', 'U.S. International Trade Commission, HTSUS (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000003', 'GB', 'TARIFF', 'uk_tariff', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('uk_tariff fixture'), 'OGL v3.0', 'HM Revenue & Customs, UK Trade Tariff (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000004', 'EU', 'TARIFF', 'xi_tariff', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('xi_tariff fixture'), 'EU reuse decision 2011/833/EU', 'European Commission, TARIC via the XI tariff (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000011', 'NG', 'IMPORT_RESTRICTION', 'ng_prohibition_list', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('ng_prohibition fixture'), 'Public sector information', 'Nigeria Customs Service import prohibition list (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000021', 'US', 'PERMIT', 'us_curated_permits', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('us_permits fixture'), 'Public domain', 'USDA APHIS permit requirements (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000031', 'US', 'SANCTIONS', 'ofac_sls', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('ofac fixture'), 'Public domain', 'OFAC Sanctions List Service (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000032', 'GB', 'SANCTIONS', 'uksl', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('uksl fixture'), 'OGL v3.0', 'FCDO UK Sanctions List (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000041', 'US', 'EXPORT_CONTROL', 'us_ear', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('ear fixture'), 'Public domain', 'BIS Commerce Control List (fixture)', 'LOADED', 'fixture-loader', false),
    ('00000000-0000-4000-8000-000000000051', 'GB', 'PREFERENTIAL_AGREEMENT', 'uk_tariff', 'fixture-2026-01', '2026-01-05T09:00:00Z', sha256('uk_fta fixture'), 'OGL v3.0', 'HM Revenue & Customs, UK Trade Tariff (fixture)', 'LOADED', 'fixture-loader', false);

-- NG raises the smartphone duty from 10% to 15% on 2026-07-01: an order on 2026-06-30 resolves
-- 10%, an order on 2026-07-01 resolves 15%.
INSERT INTO tariff_rates (dataset_id, hs_code, origin_country, measure_type, measure_code, rate_type, ad_valorem_percent, specific_amount, specific_currency, specific_unit, rate_expression, effective_from, effective_to, source_reference) VALUES
    ('00000000-0000-4000-8000-000000000001', '8517130000', '*', 'MFN', NULL, 'AD_VALOREM', 10.0000, NULL, NULL, NULL, '10%', '2022-01-01', '2026-07-01', 'ECOWAS CET 2022-2026, heading 85.17'),
    ('00000000-0000-4000-8000-000000000001', '8517130000', '*', 'MFN', NULL, 'AD_VALOREM', 15.0000, NULL, NULL, NULL, '15%', '2026-07-01', NULL, 'Fiscal Policy Measures 2026 (fixture)'),
    ('00000000-0000-4000-8000-000000000001', '8517130000', '*', 'ADDITIONAL_DUTY', 'NG_NAC_LEVY', 'AD_VALOREM', 0.5000, NULL, NULL, NULL, '0.5%', '2022-01-01', NULL, 'Nigeria Customs levy schedule (fixture)'),
    ('00000000-0000-4000-8000-000000000001', '6109100000', '*', 'MFN', NULL, 'AD_VALOREM', 20.0000, NULL, NULL, NULL, '20%', '2022-01-01', NULL, 'ECOWAS CET 2022-2026, heading 61.09'),
    ('00000000-0000-4000-8000-000000000002', '6109100012', '*', 'MFN', NULL, 'AD_VALOREM', 16.5000, NULL, NULL, NULL, '16.5%', '2020-01-01', NULL, 'HTSUS 6109.10.00.12'),
    ('00000000-0000-4000-8000-000000000002', '6109100012', 'CN', 'ADDITIONAL_DUTY', 'US_SEC_301', 'AD_VALOREM', 7.5000, NULL, NULL, NULL, '7.5%', '2020-02-14', NULL, 'HTSUS 9903.88.15 (Section 301 List 4A)'),
    ('00000000-0000-4000-8000-000000000002', '2204210000', '*', 'MFN', NULL, 'SPECIFIC', NULL, 0.0630, 'USD', 'LITRE', '6.3¢/liter', '2020-01-01', NULL, 'HTSUS 2204.21'),
    ('00000000-0000-4000-8000-000000000002', '8528720000', '*', 'MFN', NULL, 'AD_VALOREM', 5.0000, NULL, NULL, NULL, '5%', '2020-01-01', NULL, 'HTSUS 8528.72'),
    ('00000000-0000-4000-8000-000000000003', '6109100010', '*', 'MFN', NULL, 'AD_VALOREM', 12.0000, NULL, NULL, NULL, '12.00 %', '2021-01-01', NULL, 'UK Global Tariff, commodity 6109100010'),
    ('00000000-0000-4000-8000-000000000003', '6109100010', 'DE', 'PREFERENTIAL', 'UK_EU_TCA', 'AD_VALOREM', 0.0000, NULL, NULL, NULL, '0.00 %', '2021-01-01', NULL, 'UK-EU Trade and Cooperation Agreement tariff preference'),
    ('00000000-0000-4000-8000-000000000003', '0406900100', '*', 'MFN', NULL, 'COMPOUND', 6.0000, 106.4000, 'GBP', '100KG', '6.00 % + 106.40 GBP / 100 kg', '2021-01-01', NULL, 'UK Global Tariff, commodity 0406900100'),
    ('00000000-0000-4000-8000-000000000003', '1806900000', '*', 'MFN', NULL, 'UNSUPPORTED_MEASURE', NULL, NULL, NULL, NULL, '8.00 % + EA MAX 18.70 % +ADSZ', '2021-01-01', NULL, 'UK Global Tariff, commodity 1806900000'),
    ('00000000-0000-4000-8000-000000000004', '8517130000', '*', 'MFN', NULL, 'AD_VALOREM', 0.0000, NULL, NULL, NULL, '0 %', '2020-01-01', NULL, 'TARIC 8517130000, measure 103'),
    ('00000000-0000-4000-8000-000000000004', '6109100010', '*', 'MFN', NULL, 'AD_VALOREM', 12.0000, NULL, NULL, NULL, '12 %', '2020-01-01', NULL, 'TARIC 6109100010, measure 103');

INSERT INTO import_restrictions (dataset_id, hs_code, origin_country, description, effective_from, effective_to, source_reference) VALUES
    ('00000000-0000-4000-8000-000000000011', '6309', '*', 'Used clothing', '2019-01-01', NULL, 'Nigeria Customs Service import prohibition list, item 13 (fixture)');

INSERT INTO permit_requirements (dataset_id, hs_code, origin_country, permit_code, issuing_agency, document_type, effective_from, effective_to, source_reference) VALUES
    ('00000000-0000-4000-8000-000000000021', '080450', '*', 'USDA-PPQ-587', 'USDA APHIS', 'Plant import permit', '2020-01-01', NULL, '7 CFR 319.56 (fixture)'),
    ('00000000-0000-4000-8000-000000000021', '080450', '*', 'FDA-PN', 'FDA', 'Prior notice of imported food', '2020-01-01', NULL, '21 CFR 1.279 (fixture)');

INSERT INTO sanctions_list (dataset_id, list_entry_id, entity_type, primary_name, program, effective_from, effective_to, source_reference) VALUES
    ('00000000-0000-4000-8000-000000000031', 'FIXTURE-OFAC-0001', 'ENTITY', 'FIXTURE EXPORT TRADING LLC', 'SDGT', '2024-03-01', NULL, 'OFAC SDN fixture entry 0001'),
    ('00000000-0000-4000-8000-000000000031', 'FIXTURE-OFAC-0002', 'INDIVIDUAL', 'FIXTURE PERSON ONE', 'RUSSIA-EO14024', '2023-05-10', '2025-11-20', 'OFAC SDN fixture entry 0002 (delisted)'),
    ('00000000-0000-4000-8000-000000000032', 'FIXTURE-UK-0001', 'VESSEL', 'FIXTURE CARRIER', 'Russia', '2025-02-24', NULL, 'UK Sanctions List fixture entry 0001');

INSERT INTO export_controls (dataset_id, hs_code, destination_country, control_code, licence_type, effective_from, effective_to, source_reference) VALUES
    ('00000000-0000-4000-8000-000000000041', '852691', 'CN', '7A994', 'BIS_LICENCE', '2023-01-01', NULL, '15 CFR 774 Supplement 1, ECCN 7A994 (fixture)');

INSERT INTO preferential_agreements (dataset_id, agreement_code, partner_country, proof_of_origin, effective_from, effective_to, source_reference) VALUES
    ('00000000-0000-4000-8000-000000000051', 'UK_EU_TCA', 'DE', 'Statement on origin', '2021-01-01', NULL, 'UK-EU Trade and Cooperation Agreement, Article ORIG.18');
