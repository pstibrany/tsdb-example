package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	tsdb_chunkenc "github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_chunks "github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	tsdb_index "github.com/prometheus/prometheus/tsdb/index"
)

type Sample struct {
	timestamp time.Time
	value     float64
}

func main() {
	// Create new block ID and directory for block.
	blockId := ulid.MustNew(ulid.Now(), rand.Reader)
	panicOnError(os.Mkdir(blockId.String(), 0777)) // I know this is world-writeable... use your (u)masks!

	startTime := time.Now().Add(-24 * time.Hour).Truncate(24 * time.Hour) // Don't use "now" -- Prometheus may be unhappy to find block that overlaps with "current" time range.
	endTime := startTime.Add(24 * time.Hour)                              // Don't use "now" -- Prometheus may be unhappy to find block that overlaps with "current" time range.
	chunks := generateChunksFromSamples(generateSineWaveSamples(startTime, endTime, 1000))

	chunkWriter, err := tsdb_chunks.NewWriter(filepath.Join(blockId.String(), "chunks"))
	panicOnError(err)
	panicOnError(chunkWriter.WriteChunks(chunks...)) // This step is important -- it updates "Ref" field in elements.

	indexWriter, err := tsdb_index.NewWriter(context.Background(), filepath.Join(blockId.String(), "index"))
	panicOnError(err)

	// my_sine_metric{job="test"}
	mySineSeries := labels.FromStrings("__name__", "my_sine_metric", "job", "test")

	// Add all symbols, must be sorted
	var symbols []string
	for _, label := range mySineSeries {
		symbols = append(symbols, label.Name)
		symbols = append(symbols, label.Value)
	}

	sort.Strings(symbols) // In practice, you want to remove duplicates too!
	for _, sym := range symbols {
		panicOnError(indexWriter.AddSymbol(sym))
	}

	// After writing *ALL* symbols, we can start adding series. They must also be in sorted order (sorted by labels).
	// We only have single series in this example, so sorting is not a concern.
	//
	// Note that chunks must have been written to chunkWriter before adding them to series. Reason is that
	// chunk writer updates "Ref" field of each chunk.Meta, and index writer only uses that field.
	// Unfortunately AddSeries doesn't currently return error if Ref field is 0.
	panicOnError(indexWriter.AddSeries(1, mySineSeries, chunks...))

	// We are done, close everything.
	panicOnError(chunkWriter.Close())
	panicOnError(indexWriter.Close())

	// Well, almost -- we need to generate meta.json file.
	meta := tsdb.BlockMeta{
		ULID:    blockId,
		MinTime: chunks[0].MinTime,
		MaxTime: chunks[len(chunks)-1].MaxTime,
		Stats: tsdb.BlockStats{
			NumSamples:    0, // Will be computed below.
			NumSeries:     1,
			NumChunks:     uint64(len(chunks)),
			NumTombstones: 0,
		},
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{blockId},
		},
		Version: 1, // tsdb.metaVersion, https://github.com/prometheus/prometheus/blob/f93b95d16ebc644796b167827c1a7cb0a44f40e6/tsdb/block.go#L192
	}

	for _, chm := range chunks {
		meta.Stats.NumSamples += uint64(chm.Chunk.NumSamples())
	}

	panicOnError(writeMetaFile(blockId.String(), &meta))

	log.Println("Generated block", blockId)
}

func generateChunksFromSamples(samples []Sample) []tsdb_chunks.Meta {
	var (
		chunks   []tsdb_chunks.Meta
		meta     tsdb_chunks.Meta       // Metadata about current chunk
		appender tsdb_chunkenc.Appender // Appender for curent chunk. If nil, new chunk is added, and appender created for it.
	)

	for _, sample := range samples {
		// Convert unix timestamp in nanoseconds to milliseconds, as expected by Prometheus
		sampleTimestamp := sample.timestamp.UnixNano() / 1e6

		if appender == nil {
			ch := tsdb_chunkenc.NewXORChunk() // Create new chunk
			appender, _ = ch.Appender()       // Get appender for this chunk.

			meta = tsdb_chunks.Meta{
				Chunk:   ch,
				MinTime: sampleTimestamp,
			}
		}

		// Append sample to current appender.
		appender.Append(sampleTimestamp, sample.value)

		// We require that samples have increasing timestamps.
		// This will overwrite previous MaxTime.
		meta.MaxTime = sampleTimestamp

		const samplesPerChunk = 120 // This is what Prometheus uses.
		if meta.Chunk.NumSamples() == samplesPerChunk {
			// Finish this chunk.
			meta.Chunk.Compact()
			chunks = append(chunks, meta) // Add it to the slice with chunk metadata.
			appender = nil                // Create new chunk on next iteration.
		}
	} // end of for-loop iterating over samples

	if appender != nil { // If we still have appender, it means we have unfinished chunk.
		meta.Chunk.Compact()
		chunks = append(chunks, meta)
	}

	return chunks
}

func generateSineWaveSamples(startTime, endTime time.Time, points int) []Sample {
	result := make([]Sample, 0, points)

	t := startTime
	step := endTime.Sub(startTime) / time.Duration(points)

	log.Println("Start time:", startTime.UTC())
	log.Println("End time:", endTime.UTC())
	log.Println("Step:", step)

	for i := 0; i < points; i++ {
		result = append(result, Sample{
			timestamp: t,
			value:     math.Sin(float64(i) * 2 * math.Pi / float64(points)),
		})
		t = t.Add(step)
	}

	return result
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// Copied from https://github.com/prometheus/prometheus/blob/f93b95d16ebc644796b167827c1a7cb0a44f40e6/tsdb/block.go#L213
func writeMetaFile(dir string, meta *tsdb.BlockMeta) error {
	meta.Version = 1

	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, "meta.json")
	tmp := path + ".tmp"
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			log.Println("msg", "remove tmp file", "err", err.Error())
		}
	}()

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	jsonMeta, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return err
	}

	_, err = f.Write(jsonMeta)
	if err != nil {
		return tsdb_errors.NewMulti(err, f.Close()).Err()
	}

	// Force the kernel to persist the file on disk to avoid data loss if the host crashes.
	if err := f.Sync(); err != nil {
		return tsdb_errors.NewMulti(err, f.Close()).Err()
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fileutil.Replace(tmp, path)
}
