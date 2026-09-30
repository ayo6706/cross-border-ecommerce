-- MFN and additional-duty rules of the datasets, in force on @on_date, keyed on the HS code or one of
-- its prefixes and on the origin or any origin ('*'). @on_date is a calendar date, never a
-- timestamptz: a timestamptz comparison would use the session time zone (ADR 0012). The measure
-- types mirror regulatory.MeasureMFN and MeasureAdditionalDuty; PREFERENTIAL rows are not read.
-- name: ListTariffCandidates :many
SELECT id, dataset_id, hs_code, origin_country, measure_type, measure_code, rate_type, ad_valorem_percent,
       specific_amount, specific_currency, specific_unit, rate_expression, effective_from, effective_to,
       source_reference
FROM tariff_rates
WHERE dataset_id = ANY(@dataset_ids::uuid[])
  AND hs_code = ANY(@hs_codes::text[])
  AND origin_country IN (@origin::text, '*')
  AND measure_type IN ('MFN', 'ADDITIONAL_DUTY')
  AND effective_from <= @on_date::date
  AND (effective_to IS NULL OR effective_to > @on_date::date);
