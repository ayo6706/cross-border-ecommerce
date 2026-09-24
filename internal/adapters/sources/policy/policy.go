package policy

type PolicyType string

const (
	PolicyFailFast      PolicyType = "FAIL_FAST"
	PolicySkipMalformed PolicyType = "SKIP_MALFORMED"
)

type RowErrorCallback func(rowNumber int, rawRow []byte, err error)

// ErrorPolicy decides what happens to a single bad row. The run-level error
// rate limit is enforced separately by ingestion.ErrorBudget.
type ErrorPolicy struct {
	Policy     PolicyType
	OnRowError RowErrorCallback
}

// HandleRowError reports the row error and returns it when the policy is
// fail-fast. A nil return means the row should be skipped and counted.
func (p ErrorPolicy) HandleRowError(rowNumber int, rawRow []byte, err error) error {
	if p.OnRowError != nil {
		p.OnRowError(rowNumber, rawRow, err)
	}
	if p.Policy == PolicySkipMalformed {
		return nil
	}
	return err
}
