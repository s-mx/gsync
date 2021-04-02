package sync

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
)

type DestinationDataDiff struct {
	Offset		int
	Size		int
	Data		[]byte
}

type DestinationTransmitterOptions struct {
	hashOptions		PolynomialHashOptions

	N		int
}

type DestinationTransmitter struct {
	data	string
	newData []byte

	options		DestinationTransmitterOptions
	dataChannel		io.ReadWriteCloser
}

func CreateDestinationTransmitter(
	data string,
	options DestinationTransmitterOptions,
	channel io.ReadWriteCloser) *DestinationTransmitter {
	return &DestinationTransmitter{data, nil, options, channel}
}

const (
	BLOCK       = 0x1
	END         = 0x2
	_ /*ERROR*/   = 0x3
	DATA        = 0x4
	MATCHES = 0x5
)

// PerformTransmission() function performs a whole transmission process via protocol.
// TODO: describe the protocol
func (t *DestinationTransmitter) PerformTransmission(ctx context.Context) (string, error) {
	channelSize := 4096 * 256 // TODO: setup this setting.
	blockHashesChannel := make(chan DataBlockHash, channelSize)
	quitChannel := make(chan error, 1)
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go t.GenerateBlockHashes(newCtx, blockHashesChannel, quitChannel)

	var err error = nil
	currentBufferSize := 0
	bufferLimit := 1000
	bufferHashes := make([]DataBlockHash, bufferLimit)

loop:
	for {
		var blockHash DataBlockHash
		select {
		case <-ctx.Done():
			break

		case blockHash = <-blockHashesChannel:
			break

		case err = <- quitChannel:
			log.Printf("Received quit message from BuildBlockHash")
			break loop
		}

		bufferHashes[currentBufferSize] = blockHash
		currentBufferSize++
		if currentBufferSize == bufferLimit {
			err = t.SendBlocksHashes(newCtx, bufferHashes[:currentBufferSize])
			currentBufferSize = 0
			if err != nil {
				cancel()
			}
		}
	}

	if err == nil && currentBufferSize != 0 {
		err = t.SendBlocksHashes(newCtx, bufferHashes[:currentBufferSize])
		currentBufferSize = 0
	}

	if err != nil {
		err = t.SendEndingMessage()
	}

	var data string
	if data, err = t.ConstructFile(ctx); err != nil {
		return "", err
	}

	return data, err
}

type DataBlockHash struct {
	offset		int
	hash1		PolynomialHash
	md5Hash		[]byte
}

func (t *DestinationTransmitter) SendBlocksHashes(ctx context.Context, buffer []DataBlockHash) error {
	if len(buffer) == 0 {
		return nil
	}

	var err error = nil
	if _, err = t.dataChannel.Write([]byte{BLOCK}); err != nil {
		return err
	}

	buf := make([]byte, 10)
	count := binary.PutUvarint(buf, uint64(len(buffer)))
	if _, err = t.dataChannel.Write(buf[:count]); err != nil {
		return err
	}

	encoder := gob.NewEncoder(t.dataChannel)
	for _, hash := range buffer {
		select {
		case <- ctx.Done():
			return nil
		default:
		}

		if err = encoder.Encode(hash); err != nil {
			return err
		}
	}

	return nil
}

func (t *DestinationTransmitter) SendEndingMessage() error {
	_, err := t.dataChannel.Write([]byte{END})
	return err
}

func (t *DestinationTransmitter) GenerateBlockHashes(ctx context.Context, channel chan <- DataBlockHash, quit chan error) {
	currentSize := 0
	start := 0

	hash1 := PolynomialHash{0}

loop:
	for i, ch := range t.data {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		hash1 = hash1.Append(ch, t.options.hashOptions)
		leftIndex := i - t.options.N
		if leftIndex >= 0 {
			hash1 = hash1.PopLeft(int32(t.data[leftIndex]), t.options.hashOptions)
		}

		currentSize++
		if currentSize == t.options.N {
			md5Hash := md5.New()
			md5Hash.Write([]byte(t.data[start : start+currentSize]))
			md5HashValue := md5Hash.Sum(nil)
			channel <- DataBlockHash{start, hash1, md5HashValue}
			currentSize = 0
		}
	}

	close(channel)
	quit <- nil
}

func SpliceData(data *[]byte, start, end int, source []byte) {
	for i := 0; start != end; start++ {
		if start < len(*data) {
			(*data)[start] = source[i]
		} else {
			*data = append(*data, source[i])
		}

		i++
	}
}

func (t *DestinationTransmitter) WriteDataDiff(diff DestinationDataDiff) error {
	sourceOffset := 0
	if diff.Offset < len(t.newData) {
		size := len(t.newData) - diff.Offset
		if diff.Size < size {
			size = diff.Size
		}

		copy(t.newData[diff.Offset:], diff.Data[:size])
		sourceOffset += size
	}

	for _, b := range diff.Data[sourceOffset:] {
		t.newData = append(t.newData, b)
	}

	return nil
}

func (t *DestinationTransmitter) WriteMatch(match MatchDiff) error {
	return t.WriteDataDiff(DestinationDataDiff{
		Offset: match.SourceOffset,
		Size:   t.options.N,
		Data:   []byte(t.data[match.DestinationOffset : match.DestinationOffset+t.options.N]),
	})
}

func (t *DestinationTransmitter) WriteMatches(diffs []MatchDiff) error {
	for _, match := range diffs {
		if err := t.WriteMatch(match); err != nil {
			return err
		}
	}

	return nil
}

func (t *DestinationTransmitter) WriteData(
	ctx context.Context,
	diffsChannel <-chan interface{},
	quit chan<- error) {

	for {
		var diff interface{}
		select {
		case <-ctx.Done():
			return

		case diff = <-diffsChannel:
			break
		}

		switch value := diff.(type) {
		case []MatchDiff:
			if err := t.WriteMatches(value); err != nil {
				quit <- err
				return
			}

			break

		case DestinationDataDiff:
			if err := t.WriteDataDiff(value); err != nil {
				quit <- err
				return
			}

			break

		default:
			quit <- errors.New(fmt.Sprintf("got unknown diff. %T", diff))
			return
		}
	}
}

func (t *DestinationTransmitter) ProcessMatches(diffsChannel chan <- interface{}, decoder *gob.Decoder) error {

	var matchesArray []MatchDiff = nil
	if err := decoder.Decode(&matchesArray); err != nil {
		return err
	}

	diffsChannel <- matchesArray
	return nil
}

func (t *DestinationTransmitter) ProcessData(diffsChannel chan <- interface{}, decoder *gob.Decoder) error {

	var dataDiff DataDiff
	if err := decoder.Decode(&dataDiff); err != nil {
		return err
	}

	var data []byte = nil
	if err := decoder.Decode(&data); err != nil {
		return err
	}

	diffsChannel <- DestinationDataDiff{
		Offset: dataDiff.Offset,
		Size:   dataDiff.Size,
		Data:   data,
	}

	return nil
}

func (t *DestinationTransmitter) ReadIncomingStream(
	ctx context.Context,
	diffsChannel chan <- interface{}) error {

	decoder := gob.NewDecoder(t.dataChannel)

	loop:
	for {
		select {
		case <- ctx.Done():
			return ctx.Err()
		default:
		}

		var controlByte byte
		if err := decoder.Decode(&controlByte); err != nil {

		}

		switch controlByte {
		case END:
			break loop

		case MATCHES:
			if err := t.ProcessMatches(diffsChannel, decoder); err != nil {
				return err
			}
			break

		case DATA:
			if err := t.ProcessData(diffsChannel, decoder); err != nil {
				return err
			}
			break

		default:
			return errors.New(fmt.Sprintf("got unknown control byte: %d", controlByte))
		}
	}

	return nil
}

func (t* DestinationTransmitter) ConstructFile(ctx context.Context) (string, error) {
	quit := make(chan error, 1)

	// TODO: it is better to justify to size of gotten data.
	DIFFS_CHANNEL_SIZE := 100
	diffsChannel := make(chan interface{}, DIFFS_CHANNEL_SIZE)
	go t.WriteData(ctx, diffsChannel, quit)

	if err := t.ReadIncomingStream(ctx, diffsChannel); err != nil {
		return "", err
	}

	close(diffsChannel)
	<- quit
	return string(t.newData), nil
}