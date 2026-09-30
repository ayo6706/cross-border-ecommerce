package regulatory

// DatasetActivated is published when a version starts to affect decisions. PreviousDatasetID is
// the version it superseded, empty when it is the first for its key.
type DatasetActivated struct {
	EventID           string
	Dataset           Dataset
	PreviousDatasetID string
}
