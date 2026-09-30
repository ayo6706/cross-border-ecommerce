package regulatory

import "errors"

var (
	ErrInvalidDataset       = errors.New("invalid regulatory dataset")
	ErrDatasetNotFound      = errors.New("regulatory dataset not found")
	ErrDuplicateVersion     = errors.New("this dataset version is already loaded")
	ErrInvalidTransition    = errors.New("invalid regulatory dataset status transition")
	ErrInvalidReview        = errors.New("a review needs a reviewer and a note")
	ErrReviewRequired       = errors.New("regulatory dataset requires a recorded review before activation")
	ErrReviewerIsLoader     = errors.New("reviewer must not be the person who loaded the dataset")
	ErrConcurrentActivation = errors.New("another version of this dataset was activated concurrently")
	ErrInvalidRule          = errors.New("invalid regulatory rule")
	ErrInvalidSLA           = errors.New("invalid regulatory refresh SLA")
	ErrNoCoverage           = errors.New("no active regulatory dataset")
	ErrStaleCoverage        = errors.New("active regulatory dataset is older than its refresh SLA")
	ErrInvalidTariffQuery   = errors.New("invalid tariff query")
	ErrNoTariffRate         = errors.New("no tariff rate in force")
	ErrAmbiguousTariff      = errors.New("two equally specific tariff rules apply")
	ErrUnsupportedMeasure   = errors.New("tariff measure cannot be evaluated")
)
