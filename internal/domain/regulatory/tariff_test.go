package regulatory_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/shopspring/decimal"
)

func tariffParams() regulatory.TariffQueryParams {
	return regulatory.TariffQueryParams{
		Destination:   "us",
		Origin:        "cn",
		HSCode:        "8528.72.00",
		TransactionAt: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
		EvaluatedAt:   time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC),
	}
}

func tariffQuery(t *testing.T, mutate func(p *regulatory.TariffQueryParams)) regulatory.TariffQuery {
	t.Helper()
	p := tariffParams()
	if mutate != nil {
		mutate(&p)
	}
	q, err := regulatory.NewTariffQuery(p)
	if err != nil {
		t.Fatalf("tariff query: %v", err)
	}
	return q
}

func TestNewTariffQuery_NormalizesAndMapsMemberStatesToEU(t *testing.T) {
	q := tariffQuery(t, nil)
	if q.Jurisdiction() != "US" || q.Origin() != "CN" || q.HSCode() != "85287200" {
		t.Fatalf("got jurisdiction=%s origin=%s hs=%s", q.Jurisdiction(), q.Origin(), q.HSCode())
	}
	if eu := tariffQuery(t, func(p *regulatory.TariffQueryParams) { p.Destination = "GR" }); eu.Jurisdiction() != "EU" {
		t.Fatalf("GR imports resolve %s, want EU", eu.Jurisdiction())
	}
}

func TestTariffQuery_HSPrefixesLongestFirst(t *testing.T) {
	q := tariffQuery(t, nil)
	if want := []string{"85287200", "852872", "8528", "85"}; !slices.Equal(q.HSPrefixes(), want) {
		t.Fatalf("prefixes = %v, want %v", q.HSPrefixes(), want)
	}
}

// A time in New York just before midnight is already the next day in UTC; the rules of the UTC
// date apply (ADR 0012).
func TestTariffQuery_TransactionDateIsTheUTCDate(t *testing.T) {
	newYork := time.FixedZone("EDT", -4*3600)
	q := tariffQuery(t, func(p *regulatory.TariffQueryParams) {
		p.TransactionAt = time.Date(2026, 6, 30, 22, 30, 0, 0, newYork)
	})
	if want := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC); !q.TransactionDate().Equal(want) {
		t.Fatalf("transaction date = %v, want %v", q.TransactionDate(), want)
	}
}

func TestNewTariffQuery_RejectsInvalidInput(t *testing.T) {
	cases := map[string]func(p *regulatory.TariffQueryParams){
		"odd-length HS code":      func(p *regulatory.TariffQueryParams) { p.HSCode = "852" },
		"HS code over 10 digits":  func(p *regulatory.TariffQueryParams) { p.HSCode = "8528.72.00.00.1" },
		"HS code with letters":    func(p *regulatory.TariffQueryParams) { p.HSCode = "85AB" },
		"no HS code":              func(p *regulatory.TariffQueryParams) { p.HSCode = "" },
		"destination not alpha-2": func(p *regulatory.TariffQueryParams) { p.Destination = "USA" },
		"any-origin wildcard":     func(p *regulatory.TariffQueryParams) { p.Origin = "*" },
		"domestic goods":          func(p *regulatory.TariffQueryParams) { p.Origin = "US" },
		"intra-EU goods":          func(p *regulatory.TariffQueryParams) { p.Destination, p.Origin = "FR", "DE" },
		"no transaction time":     func(p *regulatory.TariffQueryParams) { p.TransactionAt = time.Time{} },
		"no evaluation time":      func(p *regulatory.TariffQueryParams) { p.EvaluatedAt = time.Time{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := tariffParams()
			mutate(&p)
			if _, err := regulatory.NewTariffQuery(p); !errors.Is(err, regulatory.ErrInvalidTariffQuery) {
				t.Fatalf("want ErrInvalidTariffQuery, got %v", err)
			}
		})
	}
}

func percent(t *testing.T, s string) *decimal.Decimal {
	t.Helper()
	d, err := decimal.NewFromString(s)
	if err != nil {
		t.Fatalf("decimal %q: %v", s, err)
	}
	return &d
}

// measure is an AD_VALOREM rule; code is empty for MFN.
func measure(t *testing.T, id, hs, origin, code, rate string) regulatory.TariffMeasure {
	t.Helper()
	m := regulatory.TariffMeasure{
		ID: id, DatasetID: "ds-1", HSCode: hs, OriginCountry: origin, MeasureType: regulatory.MeasureMFN,
		RateType: regulatory.RateAdValorem, AdValorem: percent(t, rate), RateExpression: rate + "%",
	}
	if code != "" {
		m.MeasureType, m.MeasureCode = regulatory.MeasureAdditionalDuty, code
	}
	return m
}

func measureIDs(r *regulatory.TariffResolution) []string {
	ids := make([]string, 0, len(r.Measures))
	for _, m := range r.Measures {
		ids = append(ids, m.ID)
	}
	return ids
}

func TestResolveTariff_PicksTheMostSpecificRulePerMeasure(t *testing.T) {
	q := tariffQuery(t, func(p *regulatory.TariffQueryParams) { p.HSCode = "8528720010" })
	cases := map[string]struct {
		candidates []regulatory.TariffMeasure
		want       []string
		percent    string
	}{
		"falls back to the longest prefix with a rule": {
			[]regulatory.TariffMeasure{measure(t, "chapter", "85", "*", "", "9"),
				measure(t, "8-digit", "85287200", "*", "", "5"), measure(t, "heading", "8528", "*", "", "7")},
			[]string{"8-digit"}, "5",
		},
		"the origin's rule beats any-origin even at a shorter code": {
			[]regulatory.TariffMeasure{measure(t, "any", "8528720010", "*", "", "5"),
				measure(t, "cn-heading", "8528", "CN", "", "35")},
			[]string{"cn-heading"}, "35",
		},
		"additional duties stack on MFN, MFN first": {
			[]regulatory.TariffMeasure{measure(t, "301", "85287200", "CN", "US_SEC_301", "7.5"),
				measure(t, "mfn", "85287200", "*", "", "5"), measure(t, "122", "85", "*", "US_SEC_122", "0.1"),
				measure(t, "122-narrow", "8528", "*", "US_SEC_122", "0.2")},
			[]string{"mfn", "122-narrow", "301"}, "12.7",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, err := regulatory.ResolveTariff(q, nil, tc.candidates)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if !slices.Equal(measureIDs(r), tc.want) {
				t.Fatalf("measures = %v, want %v", measureIDs(r), tc.want)
			}
			if got := r.AdValoremPercent(); !got.Equal(*percent(t, tc.percent)) {
				t.Fatalf("ad valorem = %s%%, want %s%%", got, tc.percent)
			}
		})
	}
}

func TestResolveTariff_FailsInsteadOfGuessing(t *testing.T) {
	q := tariffQuery(t, nil)
	unsupported := measure(t, "unsupported", "85287200", "*", "", "0")
	unsupported.RateType, unsupported.AdValorem = regulatory.RateUnsupported, nil
	otherSource := measure(t, "other-source", "85287200", "*", "", "6")
	otherSource.DatasetID = "ds-2"
	otherOverlay := measure(t, "other-301", "85287200", "CN", "US_SEC_301", "25")
	otherOverlay.DatasetID = "ds-2"

	cases := map[string]struct {
		candidates []regulatory.TariffMeasure
		want       error
		reason     string
	}{
		"no rule": {nil, regulatory.ErrNoTariffRate, regulatory.ReasonNoTariffRate},
		"an additional duty without an MFN rate": {
			[]regulatory.TariffMeasure{measure(t, "301", "85287200", "CN", "US_SEC_301", "7.5")},
			regulatory.ErrNoTariffRate, regulatory.ReasonNoTariffRate,
		},
		"two sources with equally specific MFN rules": {
			[]regulatory.TariffMeasure{measure(t, "mfn", "85287200", "*", "", "5"), otherSource},
			regulatory.ErrAmbiguousTariff, regulatory.ReasonAmbiguousTariff,
		},
		"two sources with equally specific US_SEC_301 rules": {
			[]regulatory.TariffMeasure{measure(t, "mfn", "85287200", "*", "", "5"),
				measure(t, "301", "85287200", "CN", "US_SEC_301", "7.5"), otherOverlay},
			regulatory.ErrAmbiguousTariff, regulatory.ReasonAmbiguousTariff,
		},
		"the applicable rule is unsupported": {
			[]regulatory.TariffMeasure{unsupported, measure(t, "heading", "8528", "*", "", "5")},
			regulatory.ErrUnsupportedMeasure, regulatory.ReasonUnsupportedMeasure,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, err := regulatory.ResolveTariff(q, nil, tc.candidates)
			if !errors.Is(err, tc.want) || r != nil {
				t.Fatalf("want %v and no resolution, got %v, %v", tc.want, r, err)
			}
			if reason, _ := regulatory.HoldReason(err); reason != tc.reason {
				t.Fatalf("hold reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}

// A tie or an unsupported rate only matters for the rule that applies: a more specific rule
// overrides both.
func TestResolveTariff_OverriddenRulesDoNotFail(t *testing.T) {
	q := tariffQuery(t, nil)
	unsupported := measure(t, "unsupported-heading", "8528", "*", "", "0")
	unsupported.RateType, unsupported.AdValorem = regulatory.RateUnsupported, nil
	tiedChapter := measure(t, "chapter-2", "85", "*", "", "9")
	tiedChapter.DatasetID = "ds-2"
	candidates := []regulatory.TariffMeasure{measure(t, "chapter-1", "85", "*", "", "9"), tiedChapter,
		unsupported, measure(t, "8-digit", "85287200", "*", "", "5")}

	r, err := regulatory.ResolveTariff(q, nil, candidates)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !slices.Equal(measureIDs(r), []string{"8-digit"}) {
		t.Fatalf("measures = %v, want [8-digit]", measureIDs(r))
	}
}

func TestTariffResolution_AdValoremPercentIsExact(t *testing.T) {
	compound := measure(t, "mfn", "0406900100", "*", "", "6")
	compound.RateType = regulatory.RateCompound
	compound.Specific = &regulatory.SpecificDuty{Amount: *percent(t, "106.40"), Currency: "GBP", Unit: "100KG"}
	r := regulatory.TariffResolution{Measures: []regulatory.TariffMeasure{compound,
		measure(t, "a", "04", "*", "A", "0.1"), measure(t, "b", "04", "*", "B", "0.2")}}
	if got := r.AdValoremPercent(); got.String() != "6.3" {
		t.Fatalf("ad valorem = %s, want 6.3", got)
	}
}
