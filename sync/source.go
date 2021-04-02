package sync

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
)

type SourceTransmitterOptions struct {
	hashOptions		PolynomialHashOptions

	N	int
}

type SourceTransmitter struct {
	hashOptions		PolynomialHashOptions

	Data	string
	N		int

	dataChannel		io.ReadWriteCloser

	matchesBuffer	[]MatchDiff
}

func CreateSourceTransmitter(data string, options SourceTransmitterOptions, channel io.ReadWriteCloser) *SourceTransmitter {
	return &SourceTransmitter{
		hashOptions: options.hashOptions,
		Data: data,
		N: options.N,
		dataChannel: channel,
	}
}

func constructHashesMap(hashes[]DataBlockHash) map[uint64][]int {
	hashesMap := make(map[uint64][]int)
	for i := range hashes {
		hash := &hashes[i]
		value := hash.hash1.value
		if _, ok := hashesMap[value]; !ok {
			hashesMap[value] = make([]int, 0)
		}

		hashesMap[value] = append(hashesMap[value], i)
	}

	return hashesMap
}

// calculateMD5Hash function calculates md5 hash of a block [offset: offset + N]
func (t *SourceTransmitter) calculateMD5Hash(offset int) []byte {
	end := offset + t.N
	return md5.New().Sum([]byte(t.Data[offset:end]))
}

func (t *SourceTransmitter) checkAndAppendDiff(
	offset int,
	hash PolynomialHash,
	hashes []DataBlockHash,
	hashesMap map[uint64][]int) (bool, int) {

	var foundHashIndexes []int = nil
	var ok bool
	if foundHashIndexes, ok = hashesMap[hash.value]; !ok {
		return false, -1
	}

	md5Hash := t.calculateMD5Hash(offset)
	for _, hashIndex := range foundHashIndexes {
		destHash := hashes[hashIndex]
		if bytes.Compare(md5Hash, destHash.md5Hash) == 0 {
			return true, destHash.offset
		}
	}

	return false, -1
}

func (t *SourceTransmitter) SendData(offset, size int) error {
	if _, err := t.dataChannel.Write([]byte{DATA}); err != nil {
		return err
	}

	buf := make([]byte, 10)
	count := binary.PutUvarint(buf, uint64(size))
	if _, err := t.dataChannel.Write(buf[:count]); err != nil {
		return err
	}

	diff := DataDiff {
		Offset: offset,
		Size:   size,
	}

	encoder := gob.NewEncoder(t.dataChannel)
	return encoder.Encode(diff)
}

// TODO: document it
func buildPolynomialHash(data string, options PolynomialHashOptions) PolynomialHash {
	hash := PolynomialHash{value: 0}
	index := 0
	for ; index != len(data); index++ {
		hash = hash.Append(int32(data[index]), options)
	}

	return hash
}

func (t *SourceTransmitter) GenerateDiffs(
	ctx context.Context,
	hashes []DataBlockHash,
	diffsChannel chan <- interface{},
	quit chan error) {
	defer func() {
		quit <- nil
		close(diffsChannel)
	}()

	if len(t.Data) < t.N {
		diffsChannel <- DataDiff{
			Offset: 0,
			Size:   len(t.Data),
		}

		return
	}

	hashesMap := constructHashesMap(hashes)
	hash := buildPolynomialHash(t.Data[:t.N], t.hashOptions)

	firstUnmatched := -1
	lastMatched := -1
	if found, destinationOffset := t.checkAndAppendDiff(0, hash, hashes, hashesMap); found {
		diffsChannel <- MatchDiff{
			SourceOffset:      0,
			DestinationOffset: destinationOffset,
		}

		lastMatched = t.N - 1
	} else {
		firstUnmatched = 0
	}

	index := t.N
	for ; index < len(t.Data); index++ {
		select {
		case <- ctx.Done():
			return
		default:
		}

		prevCh := int32(t.Data[index - t.N])
		hash = hash.PopLeft(prevCh, t.hashOptions)
		hash = hash.Append(int32(t.Data[index]), t.hashOptions)

		if found, destinationOffset := t.checkAndAppendDiff(index - t.N + 1, hash, hashes, hashesMap); found {
			diffsChannel <- MatchDiff{
				SourceOffset:      index - t.N + 1,
				DestinationOffset: destinationOffset,
			}

			if lastMatched < index - 1 {
				diffsChannel <- DataDiff{
					Offset: firstUnmatched,
					Size:   index - firstUnmatched,
				}
			}

			lastMatched = index + t.N - 1
			firstUnmatched = -1
		} else {
			if lastMatched < index && firstUnmatched == -1{
				firstUnmatched = index
			}
		}
	}

	if firstUnmatched != -1 {
		diffsChannel <- DataDiff{
			Offset: firstUnmatched,
			Size:   len(t.Data) - firstUnmatched,
		}
	}
}

func (t *SourceTransmitter) SendMatchDiffs(encoder *gob.Encoder) error {
	controlByte := MATCHES
	if err := encoder.Encode(controlByte); err != nil {
		return err
	}

	if err := encoder.Encode(t.matchesBuffer); err != nil {
		return err
	}

	t.matchesBuffer = t.matchesBuffer[:0]
	return nil
}

func (t *SourceTransmitter) PushMatchDiff(encoder *gob.Encoder, diff MatchDiff) error {
	if len(t.matchesBuffer) == cap(t.matchesBuffer) {
		if err := t.SendMatchDiffs(encoder); err != nil {
			return err
		}
	}

	t.matchesBuffer = append(t.matchesBuffer, diff)
	if len(t.matchesBuffer) != cap(t.matchesBuffer) {
		return nil
	}

	return t.SendMatchDiffs(encoder)
}

func (t *SourceTransmitter) PushDataDiff(encoder *gob.Encoder, diff DataDiff) error {
	controlByte := DATA
	if err := encoder.Encode(controlByte); err != nil {
		return err
	}

	if err := encoder.Encode(diff); err != nil {
		return err
	}

	return  encoder.Encode(t.Data[diff.Offset:diff.Size])
}

func (t *SourceTransmitter) PerformTransmission(ctx context.Context) error {
	hashes, err := t.ReceiveHashes(ctx)
	if err != nil {
		return err
	}

	diffsSize := 1024
	diffsChannel := make(chan interface{}, diffsSize)
	quit := make(chan error, 1)

	matchesBufferSize := 1024
	t.matchesBuffer = make([]MatchDiff, matchesBufferSize)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go t.GenerateDiffs(newCtx, hashes, diffsChannel, quit)
	return t.TransmitGeneratedDiffs(ctx, diffsChannel, quit)
}

func (t *SourceTransmitter) TransmitGeneratedDiffs(
	ctx context.Context,
	diffsChannel chan interface{},
	quit chan error) error {

	encoder := gob.NewEncoder(t.dataChannel)

loop:
	for {
		select {
		case diff, opened := <- diffsChannel:
			if !opened {
				break loop
			}

			if matchDiff, ok := diff.(MatchDiff); ok {
				if err := t.PushMatchDiff(encoder, matchDiff); err != nil {
					return err
				}
			} else if dataDiff, ok := diff.(DataDiff); ok {
				if err := t.PushDataDiff(encoder, dataDiff); err != nil {
					return err
				}
			} else {
				log.Printf("got an object of unknown type from GenerateDiffs: %v", diff)
				return errors.New(fmt.Sprintf("got unknown type"))
			}

			break
		case <- ctx.Done():
			return errors.New("context has closed")
		}
	}

	if err := encoder.Encode(END); err != nil {
		return err
	}

	<- quit
	return nil
}

func (t *SourceTransmitter) ReceiveHashes(ctx context.Context) ([]DataBlockHash, error) {
	hashes := make([]DataBlockHash, 1)
	decoder := gob.NewDecoder(t.dataChannel)

	for {
		select {
		case <- ctx.Done():
			return nil, errors.New("context has closed")
		default:
		}

		var controlByte byte
		if err := decoder.Decode(&controlByte); err != nil {
			return nil, err
		}

		if controlByte == END {
			break
		}

		if controlByte != BLOCK {
			return nil, errors.New(fmt.Sprintf("unknown control byte is received: %b", controlByte))
		}

		numberHashes := 0
		if err := decoder.Decode(&numberHashes); err != nil {
			return nil ,err
		}

		for i := 0; i != numberHashes; i++ {
			var hash DataBlockHash
			if err := decoder.Decode(&hash); err != nil {
				return nil, err
			}

			hashes = append(hashes, hash)
		}
	}

	return hashes, nil
}