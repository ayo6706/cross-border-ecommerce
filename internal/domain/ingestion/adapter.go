package ingestion

import "context"

type Adapter interface {
	FetchRecords(ctx context.Context, checkpoint string) (records []*RawRecord, nextCheckpoint string, err error)
}
