package place

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunBackfillJobsWriteErrorDoesNotHangSender(t *testing.T) {
	const extraJobs = 4
	jobCount := backfillWorkers + extraJobs
	jobs := make([]backfillKey, jobCount)
	for i := range jobs {
		jobs[i] = backfillKey{Index: i, key: fmt.Sprintf("k%d", i)}
	}

	var seen atomic.Int32
	writeErr := errors.New("write failed")
	state := &backfillRunState{
		outputDir: t.TempDir(),
		limiter:   &backfillLimiter{interval: 0},
	}

	done := make(chan error, 1)
	go func() {
		done <- runBackfillJobs(context.Background(), jobs, 1, state, func(context.Context, backfillKey, int, *backfillRunState) error {
			seen.Add(1)
			return writeErr
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, writeErr) {
			t.Fatalf("err = %v, want %v", err, writeErr)
		}
		if got := int(seen.Load()); got != jobCount {
			t.Fatalf("attempted %d jobs, want %d (workers stopped ranging)", got, jobCount)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("sender hung after worker write errors; attempted %d of %d jobs", seen.Load(), jobCount)
	}
}
