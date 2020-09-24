package readMicroBatching

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
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
	statisticsFileName     = "statistics.csv"
)

func TestRead(t *testing.T) {
	InitReading()
	readExitWG := sync.WaitGroup{}
	pkChan := make(chan string, ChannelBufferSize)
	readExitWG.Add(NumberOfReadInitiators)
	for i := 0; i < NumberOfReadInitiators; i++ {
		if batchedRead {
			go batchReader(pkChan, &readExitWG)
		} else {
			go discreteReader(pkChan, &readExitWG)
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
	appendResults(time.Now().Sub(start), NumberOfReads, NumberOfReadsPerBatch, batchedRead)
}

func batchReader(pkChan chan string, exitWG *sync.WaitGroup) {
	defer exitWG.Done()
	for {
		pk, moreEntries := <-pkChan
		if !moreEntries {
			break
		}
		entry, err := ReadEntry(pk)
		//fmt.Print(pk, "   ", entry.Payload, "\n")
		panicIfError(err)
		if entry.Pk != pk {
			panic("entry do not match request")
		}
	}
}

func discreteReader(pkChan chan string, exitWG *sync.WaitGroup) {
	var readPk, readPayload string
	readEntrySQL := "select pk,payload from random_read_test where pk = $1"
	defer exitWG.Done()
	for {
		pk, moreEntries := <-pkChan
		if !moreEntries {
			break
		}
		err := db.QueryRow(context.Background(), readEntrySQL, pk).Scan(&readPk, &readPayload)
		panicIfError(err)
		if readPk != pk {
			panic("entry does not match request")
		}
	}
}

func appendResults(duration time.Duration, readNum, batchSize int, batched bool) {
	f, err := os.OpenFile(statisticsFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	panicIfError(err)
	w := csv.NewWriter(f)
	batchedStr := "  discrete  "
	if batched {
		batchedStr = " batched   "
	}
	//timePer10_000Reads := duration / (readNum/1000)
	durationStr := fmt.Sprintf("%v", duration)
	batchSizeStr := fmt.Sprintf("% 10d", batchSize)
	readNumStr := fmt.Sprintf("% 10d", readNum)
	line := []string{durationStr, batchSizeStr, readNumStr, batchedStr}
	err = w.Write(line)
	panicIfError(err)
	w.Flush()
	f.Close()
}
