package batchread

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	NumberOfReadInitiators = 300
	ChannelBufferSize      = 10
	MaxPkRange             = 350_000_000
	NumberOfReads          = 3_000_000
	batchedRead            = true
	SequenceLength         = 10
	StatisticsFileName     = "statistics.csv"
)

type averageDurationType struct {
	cumulativeDuration time.Duration
	cumulativeCount    int64
	lock               sync.Mutex
}


func TestRead(t *testing.T) {
	ctx := context.Background()
	averageDurationCalculator := &averageDurationType{}
	InitReading()
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

	appendStats(averageDurationCalculator, time.Since(start), NumberOfReads, NumberOfReadsPerBatch, batchedRead)
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

func appendStats(averageDurationCalculator *averageDurationType, duration time.Duration, readNum, batchSize int, batched bool) {
	f, err := os.OpenFile(StatisticsFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	panicIfError(err)
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	var batchedStr string
	if batched {
		batchedStr = "batched"
	} else {
		batchedStr = "discrete"
	}
	batchSizeStr := strconv.Itoa(batchSize)
	readNumStr := strconv.Itoa(readNum)
	averageReadDurationStr := averageDurationCalculator.getAverage().String()
	err = w.Write([]string{duration.String(), averageReadDurationStr, batchSizeStr, readNumStr, batchedStr})
	panicIfError(err)
}

func (c *averageDurationType) addDuration(start time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cumulativeCount++
	c.cumulativeDuration += time.Since(start)
}

func (c *averageDurationType) getAverage() time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()
	average := time.Duration(int64(c.cumulativeDuration) / c.cumulativeCount)
	return average
}
