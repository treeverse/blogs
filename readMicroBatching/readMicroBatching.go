package readMicroBatching

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"os"
	"time"
)

const (
	NumberOfReadWorkers   = 10
	BatchingTimeout       = 500 * time.Microsecond
	ReadTimeout           = 100 * time.Millisecond
	NumberOfReadsPerBatch = 16
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

var db *pgxpool.Pool
var readRequestChan chan readRequest
var ErrNotFound = errors.New("not found")
var ErrReadEntryTimeout = errors.New("read entry timeout")

func ReadEntry(pk string) (*rowType, error) {
	replyChan := make(chan readResponse, 1)
	request := readRequest{pk: pk,
		replyChan: replyChan}
	readRequestChan <- request
	select {
	case response := <-replyChan:
		return response.testRow, response.err
	case <-time.After(ReadTimeout):
		return nil, ErrReadEntryTimeout
	}
}

func InitReading() {
	poolConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	panicIfError(err)
	db, err = pgxpool.ConnectConfig(context.Background(), poolConfig)
	panicIfError(err)
	readRequestChan = make(chan readRequest, 1)
	go batchingOrcestrator()
}

func batchingOrcestrator() {
	batchesChan := make(chan readMicroBatch, NumberOfReadWorkers)
	defer func() {
		close(batchesChan)
	}()
	for i := 0; i < NumberOfReadWorkers; i++ {
		go readEntriesBatch(batchesChan)
	}
	readBatch := make(readMicroBatch, 0, NumberOfReadsPerBatch)
	batchingTimer := time.NewTimer(time.Hour)
	for {
		select {
		case request, moreEntries := <-readRequestChan:
			if !moreEntries {
				return // shutdown
			}
			if len(readBatch) == 0 {
				batchingTimer.Reset(BatchingTimeout)
			}
			readBatch = append(readBatch, request)
			if len(readBatch) == NumberOfReadsPerBatch {
				batchingTimer.Stop()
				batchesChan <- readBatch
				readBatch = make(readMicroBatch, 0, NumberOfReadsPerBatch)
			}
		case <-batchingTimer.C:
			if len(readBatch) != 0 {
				batchesChan <- readBatch
				readBatch = make(readMicroBatch, 0, NumberOfReadsPerBatch)
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
		pkSlice := make([]string, 0, len(message))
		for _, readRequest := range message {
			pkSlice = append(pkSlice, readRequest.pk)
		}
		readEntriesSQL := " select pk,payload from random_read_test where pk = any ($1)"
		rows, err := db.Query(context.Background(), readEntriesSQL, pkSlice)
		if err != nil {
			panic(err)
		}
		rowsMap := make(map[string]string)
		for rows.Next() {
			var pk, payload string
			err = rows.Scan(&pk, &payload)
			panicIfError(err)
			rowsMap[pk] = payload
		}
		for _, readRequest := range message {
			var response readResponse
			payload, ok := rowsMap[readRequest.pk]
			switch {
			case ok:
				response.testRow = &rowType{Pk: readRequest.pk,
					Payload: payload}
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
