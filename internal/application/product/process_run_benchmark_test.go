package product_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	domainSource "github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/stretchr/testify/require"
)

func runBenchmarkWorkload(b *testing.B, batchSize int, workload string, recordCount int) {
	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		b.Skip("skipping benchmark: TEST_DATABASE_URL not set")
		return
	}

	env := setupLiveTestEnv(b)
	ctx := context.Background()

	sourceID := fmt.Sprintf("src-bench-%s-b%d", workload, batchSize)
	createTestSource(b, ctx, env, sourceID)

	now := time.Now().UTC()
	tOld := now.Add(-1 * time.Hour)

	// Prepare raw records
	rawRecords := make([]*domainIngestion.RawRecord, recordCount)

	switch workload {
	case "new":
		run := createTestRun(b, ctx, env, sourceID)
		for i := 0; i < recordCount; i++ {
			payload := []byte(fmt.Sprintf(`{"title": "Prod %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
				SourceID:          domainSource.ID(sourceID),
				ExternalProductID: fmt.Sprintf("ext-%d", i),
				Payload:           payload,
				SourceUpdatedAt:   &now,
				IngestionRunID:    run.ID,
				ReceivedAt:        now,
			})
			require.NoError(b, err)
			rawRecords[i] = rec
		}
		require.NoError(b, env.rawRepo.SaveBatch(ctx, rawRecords))

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			summary, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
				BatchSize:     batchSize,
				LeaseDuration: 5 * time.Minute,
				FromStart:     true,
			})
			if err != nil {
				b.Fatalf("ProcessRun failed: %v", err)
			}
			if summary.RecordsSeen != recordCount {
				b.Fatalf("expected %d processed, got %d", recordCount, summary.RecordsSeen)
			}
		}

	case "unchanged":
		// Pre-seed catalog
		seedRun := createTestRun(b, ctx, env, sourceID)
		for i := 0; i < recordCount; i++ {
			payload := []byte(fmt.Sprintf(`{"title": "Prod %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
				SourceID:          domainSource.ID(sourceID),
				ExternalProductID: fmt.Sprintf("ext-%d", i),
				Payload:           payload,
				SourceUpdatedAt:   &tOld,
				IngestionRunID:    seedRun.ID,
				ReceivedAt:        tOld,
			})
			require.NoError(b, err)
			rawRecords[i] = rec
		}
		require.NoError(b, env.rawRepo.SaveBatch(ctx, rawRecords))
		_, err := env.processor.ProcessRun(ctx, seedRun.ID, appProduct.ProcessRunOptions{
			BatchSize:     500,
			LeaseDuration: 5 * time.Minute,
		})
		require.NoError(b, err)

		// Second run: 100% unchanged
		run2 := createTestRun(b, ctx, env, sourceID)
		unchangedRecords := make([]*domainIngestion.RawRecord, recordCount)
		for i := 0; i < recordCount; i++ {
			payload := []byte(fmt.Sprintf(`{"title": "Prod %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
				SourceID:          domainSource.ID(sourceID),
				ExternalProductID: fmt.Sprintf("ext-%d", i),
				Payload:           payload,
				SourceUpdatedAt:   &now,
				IngestionRunID:    run2.ID,
				ReceivedAt:        now,
			})
			require.NoError(b, err)
			unchangedRecords[i] = rec
		}
		require.NoError(b, env.rawRepo.SaveBatch(ctx, unchangedRecords))

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			summary, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{
				BatchSize:     batchSize,
				LeaseDuration: 5 * time.Minute,
				FromStart:     true,
			})
			if err != nil {
				b.Fatalf("ProcessRun failed: %v", err)
			}
			if summary.RecordsSeen != recordCount {
				b.Fatalf("expected %d processed, got %d", recordCount, summary.RecordsSeen)
			}
		}

	case "mixed":
		// Pre-seed half
		seedRun := createTestRun(b, ctx, env, sourceID)
		for i := 0; i < recordCount/2; i++ {
			payload := []byte(fmt.Sprintf(`{"title": "Prod %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
				SourceID:          domainSource.ID(sourceID),
				ExternalProductID: fmt.Sprintf("ext-%d", i),
				Payload:           payload,
				SourceUpdatedAt:   &tOld,
				IngestionRunID:    seedRun.ID,
				ReceivedAt:        tOld,
			})
			require.NoError(b, err)
			rawRecords[i] = rec
		}
		require.NoError(b, env.rawRepo.SaveBatch(ctx, rawRecords[:recordCount/2]))
		_, err := env.processor.ProcessRun(ctx, seedRun.ID, appProduct.ProcessRunOptions{
			BatchSize:     500,
			LeaseDuration: 5 * time.Minute,
		})
		require.NoError(b, err)

		// Second run: 50% new, 25% changed, 25% unchanged
		run2 := createTestRun(b, ctx, env, sourceID)
		mixedRecords := make([]*domainIngestion.RawRecord, recordCount)
		for i := 0; i < recordCount; i++ {
			var payload []byte
			if i < recordCount/4 {
				// Unchanged
				payload = []byte(fmt.Sprintf(`{"title": "Prod %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			} else if i < recordCount/2 {
				// Changed
				payload = []byte(fmt.Sprintf(`{"title": "Prod %d - CHANGED", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			} else {
				// New
				payload = []byte(fmt.Sprintf(`{"title": "Prod %d - NEW", "body": "B", "vendor": "V", "country_code": "US"}`, i))
			}
			rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
				SourceID:          domainSource.ID(sourceID),
				ExternalProductID: fmt.Sprintf("ext-%d", i),
				Payload:           payload,
				SourceUpdatedAt:   &now,
				IngestionRunID:    run2.ID,
				ReceivedAt:        now,
			})
			require.NoError(b, err)
			mixedRecords[i] = rec
		}
		require.NoError(b, env.rawRepo.SaveBatch(ctx, mixedRecords))

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			summary, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{
				BatchSize:     batchSize,
				LeaseDuration: 5 * time.Minute,
				FromStart:     true,
			})
			if err != nil {
				b.Fatalf("ProcessRun failed: %v", err)
			}
			if summary.RecordsSeen != recordCount {
				b.Fatalf("expected %d processed, got %d", recordCount, summary.RecordsSeen)
			}
		}
	}
}

func BenchmarkProcessRun_New_Batch1(b *testing.B) {
	runBenchmarkWorkload(b, 1, "new", 1000)
}

func BenchmarkProcessRun_New_Batch500(b *testing.B) {
	runBenchmarkWorkload(b, 500, "new", 1000)
}

func BenchmarkProcessRun_Unchanged_Batch1(b *testing.B) {
	runBenchmarkWorkload(b, 1, "unchanged", 1000)
}

func BenchmarkProcessRun_Unchanged_Batch500(b *testing.B) {
	runBenchmarkWorkload(b, 500, "unchanged", 1000)
}

func BenchmarkProcessRun_Mixed_Batch1(b *testing.B) {
	runBenchmarkWorkload(b, 1, "mixed", 1000)
}

func BenchmarkProcessRun_Mixed_Batch500(b *testing.B) {
	runBenchmarkWorkload(b, 500, "mixed", 1000)
}
