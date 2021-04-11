package sync

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

type SourceTransmitterOptions struct {
	hashOptions		PolynomialHashOptions

	N       		int
	Timeout 		time.Duration
}

type SourceTransmitter struct {
	SourceTransmitterOptions

	filename		string
	Data			string

	dataChannel		net.Conn

	matchesBuffer	[]MatchDiff
}

func CreateSourceTransmitter(filename, data string, options SourceTransmitterOptions, channel net.Conn) *SourceTransmitter {
	return &SourceTransmitter{
		SourceTransmitterOptions: options,
		filename: filename,
		Data: data,
		dataChannel: channel,
	}
}

func constructHashesMap(hashes[]DataBlockHash) map[uint64][]int {
	hashesMap := make(map[uint64][]int)
	for i := range hashes {
		hash := &hashes[i]
		value := hash.Hash1.Value
		if _, ok := hashesMap[value]; !ok {
			hashesMap[value] = make([]int, 0)
		}

		hashesMap[value] = append(hashesMap[value], i)
	}

	return hashesMap
}

func (t *SourceTransmitter) updateDeadline() error {
	return t.dataChannel.SetDeadline(time.Now().Add(t.SourceTransmitterOptions.Timeout))
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
	if foundHashIndexes, ok = hashesMap[hash.Value]; !ok {
		return false, -1
	}

	md5Hash := t.calculateMD5Hash(offset)
	for _, hashIndex := range foundHashIndexes {
		destHash := hashes[hashIndex]
		if bytes.Compare(md5Hash, destHash.Md5Hash) == 0 {
			return true, destHash.Offset
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
	hash := PolynomialHash{Value: 0}
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
	controlByte := byte(DATA)
	if err := encoder.Encode(controlByte); err != nil {
		return err
	}

	if err := encoder.Encode(diff); err != nil {
		return err
	}

	return  encoder.Encode([]byte(t.Data[diff.Offset:diff.Size]))
}

func (t *SourceTransmitter) SendSyncCommand() error {
	buf := []byte("sync ")
	buf = append(buf, t.filename...)
	buf = append(buf, '\n')

	written := 0
	for written < len(buf) {
		if err := t.dataChannel.SetWriteDeadline(time.Now().Add(t.Timeout)); err != nil {
			return err
		}

		n, err := t.dataChannel.Write(buf[written:])
		written += n
		if err != nil {
			return err
		}
	}

	log.Printf("client: sent command: %s", string(buf))
	return nil
}

func (t *SourceTransmitter) PerformTransmission(ctx context.Context) error {
	if err := t.SendSyncCommand(); err != nil {
		return err
	}

	if err := t.dataChannel.SetReadDeadline(time.Now().Add(t.Timeout)); err != nil {
		return err
	}

	hashes, err := t.ReceiveHashes(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive hashes: %w", err)
	}

	diffsSize := 1024
	diffsChannel := make(chan interface{}, diffsSize)
	quit := make(chan error, 1)

	matchesBufferSize := 1024
	t.matchesBuffer = make([]MatchDiff, matchesBufferSize)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	log.Print("client: try to transmit diffs")
	go t.GenerateDiffs(newCtx, hashes, diffsChannel, quit)
	return t.TransmitGeneratedDiffs(ctx, diffsChannel, quit)
}

func (t *SourceTransmitter) TransmitGeneratedDiffs(
	ctx context.Context,
	diffsChannel chan interface{},
	quit chan error) error {

	log.Print("client: start transmitting diffs")
	encoder := gob.NewEncoder(t.dataChannel)

loop:
	for {
		select {
		case diff, opened := <- diffsChannel:
			if !opened {
				break loop
			}

			if matchDiff, ok1 := diff.(MatchDiff); ok1 {
				log.Printf("client: got match diff. %v", matchDiff)
				if err := t.PushMatchDiff(encoder, matchDiff); err != nil {
					return err
				}
			} else if dataDiff, ok2 := diff.(DataDiff); ok2 {
				log.Printf("client: got data diff. %v", dataDiff)
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

	log.Print("client: send ending message")
	if err := encoder.Encode(byte(END)); err != nil {
		return err
	}

	log.Print("client: finish transmitting diffs")
	<- quit
	return nil
}

func (t *SourceTransmitter) ReceiveHashes(ctx context.Context) ([]DataBlockHash, error) {
	hashes := make([]DataBlockHash, 0)
	decoder := gob.NewDecoder(t.dataChannel)

	log.Print("client: try to receive hashes")
	for {
		select {
		case <- ctx.Done():
			return nil, errors.New("context has closed")
		default:
		}

		if err := t.updateDeadline(); err != nil {
			return nil, err
		}

		var controlByte byte
		if err := decoder.Decode(&controlByte); err != nil {
			return nil, fmt.Errorf("failed to receive and decode control byte: %w", err)
		}

		log.Printf("client: got control byte: %b", controlByte)

		if controlByte == END {
			log.Print("client: end control byte received")
			break
		}

		if controlByte != BLOCK {
			return nil, fmt.Errorf("unknown control byte is received: %b", controlByte)
		}

		numberHashes := 0
		if err := decoder.Decode(&numberHashes); err != nil {
			return nil ,err
		}

		log.Printf("client: got numberHashes: %d", numberHashes)
		for i := 0; i != numberHashes; i++ {
			var hash DataBlockHash
			if err := decoder.Decode(&hash); err != nil {
				return nil, fmt.Errorf("failed to decode %d-th hash: %w", i, err)
			}

			log.Printf("client: got DataBlockHash: %v", hash)
			hashes = append(hashes, hash)
		}
	}

	log.Printf("client: hashes were received. size: %d", len(hashes))
	return hashes, nil
}