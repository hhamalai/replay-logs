package main

import "time"
import "fmt"

// Computes simple moving averages from a given time frame

type TimedBytes struct {
	ts time.Time
	bytes int
}

// TimedMetric compute rolling window averages (e.g. bytes/sec)
type TimedMetric struct {
	measurements []TimedBytes
	windowSize time.Duration
}

// Prune measurements not in the window
func (tm *TimedMetric) Prune(t time.Time) {
	if len(tm.measurements) > 0 {
		for i, measurement := range tm.measurements {
			if measurement.ts.After(t.Add(-tm.windowSize)) {
				tm.measurements = tm.measurements[i:]
				break
			}
		}
	}
}

// Add prunes and adds value with current timestamp
func (tm *TimedMetric) Add(bytes int) {
	t := time.Now()
	tm.Prune(t)
	tm.measurements = append(tm.measurements, TimedBytes{
		ts:    t,
		bytes: bytes,
	})
}

// Value prunes and returns current formatted value
func (tm *TimedMetric) Value() (int, string) {
	t := time.Now()
	tm.Prune(t)
	totalBytes := 0

	for _, measurement := range tm.measurements {
		totalBytes += measurement.bytes
	}
	bs := totalBytes

	result := fmt.Sprintf("%d B/s", totalBytes)

	if totalBytes >= 1000 {
		totalBytes /= 1000
		result = fmt.Sprintf("%d kB/s", totalBytes)
	}

	if totalBytes >= 1000 {
		totalBytes /= 1000
		result = fmt.Sprintf("%d MB/s", totalBytes)
	}

	return bs, result
}

