package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

const (
	NumberOfReadInitiators = 300
	ChannelBufferSize      = 10
	MaxPkRange             = 350_000_000
	NumberOfReads          = 3_000_000
	batchedRead            = true
	SequenceLength         = 10
)

func TestRead(t *testing.T) {
	InitReading()
	readExitWG := sync.WaitGroup{}
	pkChan := make(chan string, ChannelBufferSize)
	readExitWG.Add(NumberOfReadInitiators)
	for i := 0; i < NumberOfReadInitiators; i++ {
		if batchedRead {
			go readInitiator(pkChan, &readExitWG)
		} else {
			go directReader(pkChan, &readExitWG)
		}
	}
	for i := 0; i < NumberOfReads; i += SequenceLength {
		r := rand.Int31n(MaxPkRange)
		for j := 0; j < SequenceLength; j++ {
			pk := fmt.Sprintf("%020d", r)
			pkChan <- pk
		}
	}
	close(pkChan)
	readExitWG.Wait()
}

func readInitiator(pkChan chan string, exitWG *sync.WaitGroup) {
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

func directReader(pkChan chan string, exitWG *sync.WaitGroup) {
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
