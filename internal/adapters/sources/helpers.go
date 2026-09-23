package sources

import (
	"context"
	"errors"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

// fetchWithRecords delegates a typed FetchRequest to an underlying FetchRecords function.
func fetchWithRecords(
	ctx context.Context,
	req ingestion.FetchRequest,
	fetchRecords func(ctx context.Context, checkpoint string) ([]*ingestion.RawRecord, string, error),
) (ingestion.FetchResult, error) {
	records, nextCPStr, err := fetchRecords(ctx, req.Checkpoint.String())
	if err != nil {
		return ingestion.FetchResult{}, err
	}
	nextCP := ingestion.NewCheckpoint(nextCPStr)
	return ingestion.FetchResult{
		Records:        records,
		NextCheckpoint: nextCP,
		HasMore:        !nextCP.IsEmpty(),
	}, nil
}

// probeWithFetch executes a pre-flight probe using an adapter's Fetch method with BatchSize=1.
func probeWithFetch(ctx context.Context, adapter ingestion.Adapter) (*ingestion.SourceProbeResult, error) {
	start := time.Now()
	res, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 1})
	latency := time.Since(start)
	if err != nil {
		return &ingestion.SourceProbeResult{
			Reachable:     !errors.Is(err, ingestion.ErrAdapterUnavailable),
			Authenticated: !errors.Is(err, ingestion.ErrAuthenticationFailed),
			Latency:       latency,
		}, err
	}

	sampleID := ""
	if len(res.Records) > 0 {
		sampleID = res.Records[0].ExternalProductID
	}

	return &ingestion.SourceProbeResult{
		Reachable:        true,
		Authenticated:    true,
		SampleCount:      len(res.Records),
		SampleExternalID: sampleID,
		NextCheckpoint:   res.NextCheckpoint,
		Latency:          latency,
	}, nil
}
