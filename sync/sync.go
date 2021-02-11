package sync

import (
	"bytes"
	"crypto/md5"
	"io"
)

// Source    |     Destination
//
//
//

type DataChannel interface {
	io.ReadWriteCloser
}

type DiffType int

const (
	DataDiffType 	DiffType = iota
	MatchDiffType
)

type DataDiff struct {
	Offset		int64
	Data		[]byte
	Size		int
}

type MatchDiff struct {
	SourceOffset		int64
	DestinationOffset	int64
}

type Diff struct {
	Type		DiffType
	DataDiff	DataDiff
	MatchDiff	MatchDiff
}

type SourceTransmitterOptions struct {
	hashOptions		PolynomialHashOptions

	N	int
}

type SourceTransmitter struct {
	hashOptions		PolynomialHashOptions

	Data	string
	N		int
}

func CreateSourceTransmitter(data string, options SourceTransmitterOptions) *SourceTransmitter {
	return &SourceTransmitter{
		hashOptions: options.hashOptions,
		Data: data,
		N: options.N,
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

func (t *SourceTransmitter) calculateMD5Hash(offset int64) []byte {
	end := offset + int64(t.N)
	return md5.New().Sum([]byte(t.Data[offset:end]))
}

func (t *SourceTransmitter) checkAndAppendDiff(
	offset int64,
	hash PolynomialHash,
	hashes []DataBlockHash,
	hashesMap map[uint64][]int,
	diffs []Diff) bool {

	var foundHashIndexes []int = nil
	var ok bool
	if foundHashIndexes, ok = hashesMap[hash.value]; !ok {
		return false
	}

	md5Hash := t.calculateMD5Hash(offset)
	for _, hashIndex := range foundHashIndexes {
		destHash := hashes[hashIndex]
		if bytes.Compare(md5Hash, destHash.md5Hash) == 0 {
			diffs = append(diffs, Diff{
				Type: MatchDiffType,
				DataDiff: DataDiff{int64(0), nil, 0},
				MatchDiff: MatchDiff{
					SourceOffset:      offset,
					DestinationOffset: destHash.offset,
				},
			})

			return true
		}
	}

	return false
}

func (t *SourceTransmitter) MatchBlockHashesAndStreamDiff(hashes []DataBlockHash) []Diff {
	if len(t.Data) < t.N {
		// There cannot be any matching block.
		return []Diff{
			{
				Type: DataDiffType,
				DataDiff: DataDiff{0, []byte(t.Data), len(t.Data)},
				MatchDiff: MatchDiff{},
			},
		}
	}

	hashesMap := constructHashesMap(hashes)
	hash := PolynomialHash{value: 0}
	index := 0
	for ; index != len(t.Data) && index != t.N; index++ {
		hash = hash.Append(int32(t.Data[index]), t.hashOptions)
	}

	diff := make([]Diff, 0)
	firstUnmatched := -1
	lastMatched := -1
	if found := t.checkAndAppendDiff(0, hash, hashes, hashesMap, diff); found {
		lastMatched = t.N - 1
	} else {
		firstUnmatched = 0
	}

	index++
	for ; index < len(t.Data); index++ {
		prevCh := int32(t.Data[index - t.N])
		hash = hash.PopLeft(prevCh, t.hashOptions)
		hash = hash.Append(int32(t.Data[index]), t.hashOptions)
		if found := t.checkAndAppendDiff(int64(index - t.N + 1), hash, hashes, hashesMap, diff); found {
			if lastMatched < index - 1 {
				diff = append(diff, Diff{
					Type: DataDiffType,
					MatchDiff: MatchDiff{},
					DataDiff: DataDiff{
						Offset: int64(firstUnmatched),
						Data:   []byte(t.Data[firstUnmatched:index]),
						Size:   index - firstUnmatched,
					},
				})
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
		diff = append(diff, Diff{
			Type: DataDiffType,
			MatchDiff: MatchDiff{},
			DataDiff: DataDiff{
				Offset: int64(firstUnmatched),
				Data:   []byte(t.Data[firstUnmatched:]),
				Size:   len(t.Data) - firstUnmatched,
			},
		})
	}

	return diff
}

type DestinationTransmitterOptions struct {
	hashOptions		PolynomialHashOptions

	N		int64
}

type DestinationTransmitter struct {
	data	string

	options		DestinationTransmitterOptions
}

func CreateDestinationTransmitter(data string, options DestinationTransmitterOptions) *DestinationTransmitter {
	return &DestinationTransmitter{data, options}
}

type DataBlockHash struct {
	offset		int64
	hash1		PolynomialHash
	md5Hash		[]byte
}

func (t *DestinationTransmitter) BuildBlockHashes() []DataBlockHash {
	dataBlockHashes := make([]DataBlockHash, 0)
	currentSize := int64(0)
	start := int64(0)

	hash1 := PolynomialHash{0}
	for i, ch := range t.data {
		hash1 = hash1.Append(ch, t.options.hashOptions)
		leftIndex := int64(i) - t.options.N
		if leftIndex >= 0 {
			hash1 = hash1.PopLeft(int32(t.data[leftIndex]), t.options.hashOptions)
		}

		currentSize++
		if currentSize == t.options.N {
			md5Hash := md5.New()
			md5Hash.Write([]byte(t.data[start : start+currentSize]))
			md5HashValue := md5Hash.Sum(nil)
			dataBlockHashes = append(dataBlockHashes, DataBlockHash{start, hash1, md5HashValue})
			currentSize = 0
		}
	}

	return dataBlockHashes
}

func (t *DestinationTransmitter) ConstructOriginalData(diffs []Diff) string {
	return ""
}