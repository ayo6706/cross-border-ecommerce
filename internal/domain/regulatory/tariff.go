package regulatory

import (
	"cmp"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// MeasureType values match the measure_type CHECK of tariff_rates (migration 000022).
type MeasureType string

const (
	MeasureMFN            MeasureType = "MFN"
	MeasureAdditionalDuty MeasureType = "ADDITIONAL_DUTY"
)

// RateType values match the rate_type CHECK of tariff_rates.
type RateType string

const (
	RateAdValorem   RateType = "AD_VALOREM"
	RateSpecific    RateType = "SPECIFIC"
	RateCompound    RateType = "COMPOUND"
	RateUnsupported RateType = "UNSUPPORTED_MEASURE"
)

// hsCodePattern matches the hs_code domain of migration 000022.
var hsCodePattern = regexp.MustCompile(`^(\d{2}){1,5}$`)

// ParseHSCode accepts a printed code (8528.72.00) and returns its digits: 2 to 10, in pairs.
func ParseHSCode(s string) (string, error) {
	code := strings.NewReplacer(".", "", " ", "").Replace(s)
	if !hsCodePattern.MatchString(code) {
		return "", fmt.Errorf("%w: HS code %q is not 2-10 digits in pairs", ErrInvalidTariffQuery, s)
	}
	return code, nil
}

// TariffQuery asks for the duty on goods of one origin entering one jurisdiction. The UTC date of
// the transaction selects the rules in force (the law of that day); the evaluation time selects the
// dataset versions and judges their freshness (what was known when the decision was made). Its
// fields are unexported so that every query comes from NewTariffQuery.
type TariffQuery struct {
	jurisdiction  string
	origin        string
	hsCode        string
	transactionAt time.Time
	evaluatedAt   time.Time
}

type TariffQueryParams struct {
	Destination   string
	Origin        string
	HSCode        string
	TransactionAt time.Time
	EvaluatedAt   time.Time
}

func NewTariffQuery(p TariffQueryParams) (TariffQuery, error) {
	destination, ok := parseCountry(p.Destination)
	if !ok {
		return TariffQuery{}, fmt.Errorf("%w: destination %q is not an alpha-2 code", ErrInvalidTariffQuery, p.Destination)
	}
	origin, ok := parseCountry(p.Origin)
	if !ok {
		return TariffQuery{}, fmt.Errorf("%w: origin %q is not an alpha-2 code", ErrInvalidTariffQuery, p.Origin)
	}
	jurisdiction := importJurisdiction(destination)
	if importJurisdiction(origin) == jurisdiction {
		return TariffQuery{}, fmt.Errorf("%w: goods of %s origin are not imported into %s",
			ErrInvalidTariffQuery, origin, jurisdiction)
	}
	hsCode, err := ParseHSCode(p.HSCode)
	if err != nil {
		return TariffQuery{}, err
	}
	if p.TransactionAt.IsZero() || p.EvaluatedAt.IsZero() {
		return TariffQuery{}, fmt.Errorf("%w: transaction and evaluation times are required", ErrInvalidTariffQuery)
	}
	return TariffQuery{
		jurisdiction:  jurisdiction,
		origin:        origin,
		hsCode:        hsCode,
		transactionAt: p.TransactionAt.UTC(),
		evaluatedAt:   p.EvaluatedAt.UTC(),
	}, nil
}

// Jurisdiction is the destination's import jurisdiction: EU for a member state.
func (q TariffQuery) Jurisdiction() string   { return q.jurisdiction }
func (q TariffQuery) Origin() string         { return q.origin }
func (q TariffQuery) HSCode() string         { return q.hsCode }
func (q TariffQuery) EvaluatedAt() time.Time { return q.evaluatedAt }

// TransactionDate is the UTC calendar date of the transaction, whatever location its time was
// given in: rules take effect on dates, and a local date shifts the midnight boundary (ADR 0012).
func (q TariffQuery) TransactionDate() time.Time {
	return calendarDate(q.transactionAt)
}

// HSPrefixes are the query's code and every shorter code a rule may be keyed on, longest first.
func (q TariffQuery) HSPrefixes() []string {
	prefixes := make([]string, 0, len(q.hsCode)/2)
	for n := len(q.hsCode); n >= 2; n -= 2 {
		prefixes = append(prefixes, q.hsCode[:n])
	}
	return prefixes
}

// SpecificDuty is an amount per unit of quantity (6.3¢/litre is 0.063 USD per LITRE).
type SpecificDuty struct {
	Amount   decimal.Decimal
	Currency string
	Unit     string
}

// TariffMeasure is one tariff_rates row. AdValorem is set for AD_VALOREM and COMPOUND rates,
// Specific for SPECIFIC and COMPOUND; an UNSUPPORTED_MEASURE has neither, only its expression.
type TariffMeasure struct {
	ID              string
	DatasetID       string
	HSCode          string
	OriginCountry   string
	MeasureType     MeasureType
	MeasureCode     string
	RateType        RateType
	AdValorem       *decimal.Decimal
	Specific        *SpecificDuty
	RateExpression  string
	SourceReference string
	Validity        Validity
}

// TariffResolution is the duty in force: the MFN rate and every additional duty stacked on it,
// and the dataset versions in force when the query was evaluated.
type TariffResolution struct {
	Query    TariffQuery
	Datasets []*Dataset
	Measures []TariffMeasure
}

// AdValoremPercent sums the measures' ad valorem components. Specific components depend on
// quantity and currency, so they stay on their measures.
func (r *TariffResolution) AdValoremPercent() decimal.Decimal {
	total := decimal.Zero
	for i := range r.Measures {
		if r.Measures[i].AdValorem != nil {
			total = total.Add(*r.Measures[i].AdValorem)
		}
	}
	return total
}

type measureKey struct {
	measureType MeasureType
	code        string
}

// ResolveTariff keeps, for the MFN rate and for each additional duty programme, the most specific
// candidate: a rule for the query's origin before a rule for any origin, then the longest HS code.
// candidates are the rules in force for the query, from any of the datasets. A missing MFN rate,
// two equally specific rules for one measure, or an unsupported rate is an error, never a zero.
func ResolveTariff(q TariffQuery, datasets []*Dataset, candidates []TariffMeasure) (*TariffResolution, error) {
	best, tied := mostSpecific(q, candidates)
	if _, ok := best[measureKey{measureType: MeasureMFN}]; !ok {
		return nil, fmt.Errorf("%w: HS %s from %s into %s on %s", ErrNoTariffRate,
			q.hsCode, q.origin, q.jurisdiction, q.TransactionDate().Format(time.DateOnly))
	}
	keys := slices.SortedFunc(maps.Keys(best), compareMeasureKeys)
	measures := make([]TariffMeasure, 0, len(keys))
	for _, k := range keys {
		m := candidates[best[k]]
		if tied[k] {
			return nil, fmt.Errorf("%w: %s %s for HS %s from %s has two equally specific rules (%s in dataset %s and another)",
				ErrAmbiguousTariff, k.measureType, k.code, q.hsCode, q.origin, m.ID, m.DatasetID)
		}
		if m.RateType == RateUnsupported {
			return nil, fmt.Errorf("%w: %s %s rule %s is %q", ErrUnsupportedMeasure,
				k.measureType, k.code, m.ID, m.RateExpression)
		}
		measures = append(measures, m)
	}
	return &TariffResolution{Query: q, Datasets: datasets, Measures: measures}, nil
}

// mostSpecific indexes the most specific candidate per measure, and marks the measures whose most
// specific candidate has an equally specific rival.
func mostSpecific(q TariffQuery, candidates []TariffMeasure) (best map[measureKey]int, tied map[measureKey]bool) {
	best, tied = map[measureKey]int{}, map[measureKey]bool{}
	for i := range candidates {
		key := measureKey{candidates[i].MeasureType, candidates[i].MeasureCode}
		j, seen := best[key]
		if !seen {
			best[key] = i
			continue
		}
		switch c := compareSpecificity(q, &candidates[i], &candidates[j]); {
		case c > 0:
			best[key], tied[key] = i, false
		case c == 0:
			tied[key] = true
		}
	}
	return best, tied
}

// compareSpecificity is positive when a is more specific than b for the query.
func compareSpecificity(q TariffQuery, a, b *TariffMeasure) int {
	return cmp.Or(
		cmp.Compare(boolRank(a.OriginCountry == q.origin), boolRank(b.OriginCountry == q.origin)),
		cmp.Compare(len(a.HSCode), len(b.HSCode)),
	)
}

func boolRank(b bool) int {
	if b {
		return 1
	}
	return 0
}

// compareMeasureKeys orders the MFN rate first, then additional duties by programme code.
func compareMeasureKeys(a, b measureKey) int {
	return cmp.Or(
		cmp.Compare(boolRank(b.measureType == MeasureMFN), boolRank(a.measureType == MeasureMFN)),
		strings.Compare(a.code, b.code),
	)
}
