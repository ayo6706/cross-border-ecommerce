package ingestion

import (
	"context"
	"time"
)

// FetchRequest encapsulates the parameters for an incremental ingestion fetch.
type FetchRequest struct {
	Checkpoint Checkpoint
	BatchSize  int
}

// FetchResult encapsulates the retrieved records and pagination state.
type FetchResult struct {
	Records        []*RawRecord
	NextCheckpoint Checkpoint
	HasMore        bool
}

// SourceProbeResult encapsulates diagnostic pre-flight health check results.
type SourceProbeResult struct {
	Reachable        bool          `json:"reachable"`
	Authenticated    bool          `json:"authenticated"`
	SampleCount      int           `json:"sample_count"`
	SampleExternalID string        `json:"sample_external_id"`
	NextCheckpoint   Checkpoint    `json:"next_checkpoint"`
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
