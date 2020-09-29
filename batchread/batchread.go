package batchread

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	BatchingTimeout = 500 * time.Microsecond
	ReadTimeout     = 500 * time.Millisecond
)

type readRequest struct {
	pk        string
	replyChan chan readResponse
}

type readMicroBatch []readRequest

type rowType struct {
	Pk      string
	Payload string
}

type readResponse struct {
	testRow *rowType
	err     error
}

var (
	db              *pgxpool.Pool
	readRequestChan chan readRequest

	ErrNotFound         = errors.New("not found")
	ErrReadEntryTimeout = errors.New("read entry timeout")
)

func ReadEntry(pk string) (*rowType, error) {
	replyChan := make(chan readResponse, 1)
	request := readRequest{
		pk:        pk,
		replyChan: replyChan,
	}
	readRequestChan <- request
	select {
	case response := <-replyChan:
		return response.testRow, response.err
	case <-time.After(ReadTimeout):
		return nil, ErrReadEntryTimeout
	}
}

func InitReading(numberOfReadsPerBatch, numberOfReadWorkers int, numberOfConnections int32) {
	config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	panicIfError(err)
	config.MaxConns = numberOfConnections
	db, err = pgxpool.ConnectConfig(context.Background(), config)
	panicIfError(err)
	readRequestChan = make(chan readRequest, 1)
	go batchingOrchestrator(numberOfReadsPerBatch, numberOfReadWorkers)
}

func batchingOrchestrator(numberOfReadsPerBatch, numberOfReadWorkers int) {
	batchesChan := make(chan readMicroBatch, numberOfReadWorkers)
	defer close(batchesChan)
	for i := 0; i < numberOfReadWorkers; i++ {
		go readEntriesBatch(batchesChan)
	}
	readBatch := make(readMicroBatch, 0, numberOfReadsPerBatch)
	// batching timer will be set on the first request
	batchingTimer := time.NewTimer(0)
	batchingTimer.Stop()
	for {
		select {
		case request, moreEntries := <-readRequestChan:
			if !moreEntries {
				return // shutdown
			}
			if len(readBatch) == 0 { // first entry in batch
				batchingTimer.Reset(BatchingTimeout)
			}
			readBatch = append(readBatch, request)
			if len(readBatch) == numberOfReadsPerBatch {
				batchingTimer.Stop()
				batchesChan <- readBatch
				readBatch = make(readMicroBatch, 0, numberOfReadsPerBatch)
			}
		case <-batchingTimer.C:
			if len(readBatch) != 0 {
				batchesChan <- readBatch
				readBatch = make(readMicroBatch, 0, numberOfReadsPerBatch)
			}
		}
	}
}

func readEntriesBatch(inputBatchChan chan readMicroBatch) {
	for {
		message, more := <-inputBatchChan
		if !more {
			return
		}
		pkSlice := make([]string, len(message))
		for i, readRequest := range message {
			pkSlice[i] = readRequest.pk
		}
		readEntriesSQL := "select pk,payload from random_read_test where pk = any ($1)"
		rows, err := db.Query(context.Background(), readEntriesSQL, pkSlice)
		panicIfError(err)
		rowsMap := make(map[string]string)
		for rows.Next() {
			var pk, payload string
			err = rows.Scan(&pk, &payload)
			panicIfError(err)
			rowsMap[pk] = payload
		}
		panicIfError(err)
		for _, readRequest := range message {
			var response readResponse
			payload, ok := rowsMap[readRequest.pk]
			switch {
			case ok:
				response.testRow = &rowType{
					Pk:      readRequest.pk,
					Payload: payload,
				}
				response.err = nil
			case err != nil:
				response.err = err
			default:
				response.err = ErrNotFound
			}
			readRequest.replyChan <- response
			close(readRequest.replyChan)
		}
	}
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}
