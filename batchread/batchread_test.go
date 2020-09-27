package batchread

import (
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	batchedRead            = true
	NumberOfReadsPerBatch  = 16
	NumberOfConnections    = 10
	NumberOfReads          = 3_000_000
	MaxPkRange             = 350_000_000
	NumberOfReadWorkers    = 10
	NumberOfReadInitiators = 300
	SequenceLength         = 10
	StatisticsFileName     = "statistics.csv"
	ChannelBufferSize      = 10
)

type averageDurationType struct {
	cumulativeDuration int64
	counter            int64
	maxDuration        int64
	minDuration        int64
	lock               sync.Mutex
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	averageDurationCalculator := &averageDurationType{minDuration: math.MaxInt64}
	InitReading(NumberOfReadsPerBatch, NumberOfReadWorkers, NumberOfConnections)
	pkChan := make(chan string, ChannelBufferSize)
	var readExitWG sync.WaitGroup
	readExitWG.Add(NumberOfReadInitiators)
	for i := 0; i < NumberOfReadInitiators; i++ {
		if batchedRead {
			go batchReader(ctx, &readExitWG, pkChan, averageDurationCalculator)
		} else {
			go discreteReader(ctx, &readExitWG, pkChan, averageDurationCalculator)
		}
	}
	start := time.Now()
	for i := 0; i < NumberOfReads; i += SequenceLength {
		r := rand.Int31n(MaxPkRange)
		for j := 0; j < SequenceLength; j++ {
			pk := fmt.Sprintf("%020d", r)
			pkChan <- pk
		}
	}
	close(pkChan)
	readExitWG.Wait()

	collectStats(averageDurationCalculator, time.Since(start), NumberOfReads, NumberOfReadsPerBatch, batchedRead)
}

func batchReader(ctx context.Context, wg *sync.WaitGroup, pkChan chan string, averageDurationCalculator *averageDurationType) {
	defer wg.Done()
	for {
		select {
		case pk, ok := <-pkChan:
			if !ok {
				return
			}
			startRead := time.Now()
			entry, err := ReadEntry(pk)
			panicIfError(err)
			averageDurationCalculator.addDuration(startRead)
			if entry.Pk != pk {
				panic("entry do not match request")
			}
		case <-ctx.Done():
			return
		}
	}
}

func discreteReader(ctx context.Context, wg *sync.WaitGroup, pkChan chan string, averageDurationCalculator *averageDurationType) {
	defer wg.Done()
	const readEntrySQL = "select pk,payload from random_read_test where pk = $1"
	for {
		select {
		case pk, ok := <-pkChan:
			if !ok {
				return
			}
			startRead := time.Now()
			var readPk, readPayload string
			err := db.QueryRow(ctx, readEntrySQL, pk).Scan(&readPk, &readPayload)
			panicIfError(err)
			averageDurationCalculator.addDuration(startRead)
			if readPk != pk {
				panic("entry does not match request")
			}
		case <-ctx.Done():
			return
		}
	}
}

func collectStats(averageDurationCalculator *averageDurationType, duration time.Duration, readNum, batchSize int, batched bool) {
	var newFile bool = false
	if _, err := os.Stat(StatisticsFileName); os.IsNotExist(err) {
		newFile = true
	}
	f, err := os.OpenFile(StatisticsFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	panicIfError(err)
	defer f.Close()

	w := csv.NewWriter(f)
	if newFile {
		err = w.Write([]string{"run type", "batch size", "duration", "average duearion", "max duearion", "min duearion", "number of reads"})
		panicIfError(err)
	}
	runTypeStr := "  discrete  "
	if batched {
		runTypeStr = " batched   "
	}
	durationStr := fmt.Sprintf("%v", duration)
	batchSizeStr := fmt.Sprintf("% 10d", batchSize)
	readNumStr := fmt.Sprintf("% 10d", readNum)
	averageReadDurationStr := fmt.Sprintf("  %v  ", averageDurationCalculator.getAverage())
	maxDurationStr := fmt.Sprintf("  %v  ", averageDurationCalculator.getMaxDuration())
	minDurationStr := fmt.Sprintf("  %v  ", averageDurationCalculator.getMinDuration())

	line := []string{runTypeStr, batchSizeStr, durationStr, averageReadDurationStr, maxDurationStr, minDurationStr, readNumStr}
	err = w.Write(line)
	panicIfError(err)
	w.Flush()
	f.Close()
}

func (c *averageDurationType) addDuration(start time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.counter++
	int64Duration := int64(time.Since(start))
	c.cumulativeDuration += int64Duration
	if c.maxDuration < int64Duration {
		c.maxDuration = int64Duration
	}
	if c.minDuration > int64Duration {
		c.minDuration = int64Duration
	}
}

func (c *averageDurationType) getAverage() time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()
	average := time.Duration(c.cumulativeDuration / c.counter)
	return average
}
func (c *averageDurationType) getMaxDuration() time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()
	return time.Duration(c.maxDuration)
}
func (c *averageDurationType) getMinDuration() time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()
	return time.Duration(c.minDuration)
}
