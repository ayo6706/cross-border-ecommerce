package ingestion

import (
	"context"
	"time"
)

// FetchRequest encapsulates the parameters for an incremental ingestion fetch.
//
// BatchSize is a hint. Adapters whose checkpoint meaning depends on a fixed
// page size (e.g. page-number pagination) ignore it, because changing the page
// size between batches would skip or repeat records on resume.
type FetchRequest struct {
	Checkpoint string
	BatchSize  int
}

// FetchResult encapsulates the retrieved records and pagination state.
// Failed counts rows the adapter skipped under its row error policy.
type FetchResult struct {
	Records        []*RawRecord
	Failed         int
	NextCheckpoint string
	HasMore        bool
}

// SourceProbeResult encapsulates diagnostic pre-flight health check results.
type SourceProbeResult struct {
	Reachable        bool          `json:"reachable"`
	Authenticated    bool          `json:"authenticated"`
	SampleCount      int           `json:"sample_count"`
	SampleExternalID string        `json:"sample_external_id"`
	NextCheckpoint   string        `json:"next_checkpoint"`
	Latency          time.Duration `json:"latency"`
}

// Adapter defines the port for incremental source ingestion.
type Adapter interface {
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
}

// ProbingAdapter extends Adapter with pre-flight dry-run diagnostic capabilities.
type ProbingAdapter interface {
	Adapter
	Probe(ctx context.Context) (*SourceProbeResult, error)
}
