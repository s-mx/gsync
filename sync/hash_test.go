package sync

import (
	"math/rand"
	"testing"
)

func generateData(size int) string {
	var arr []byte = nil
	for i := 0; i != size; i++ {
		arr = append(arr, byte(rand.Int31n(127)))
	}

	return string(arr)
}

func calculatePolynomialHashByLoop(data string, start, end int, options PolynomialHashOptions) uint64 {
	value := int64(0)
	for start != end {
		value = (value * int64(options.parameter)) % int64(options.modulo)
		value += int64(data[start])
		value %= int64(options.modulo)
		start++
	}

	return uint64(value)
}

func TestPolynomialHash(t *testing.T) {
	a := 123456789
	mod := 1_000_000_000 + 7
	windowLength := 10000
	SIZE := 100000

	options := CreatePolynomialHashOptions(a, mod, windowLength)
	hash := PolynomialHash{Value: 0}
	data := generateData(SIZE)

	windowStart := 0
	i := 0
	for ; i != windowLength; i++ {
		hash = hash.Append(int32(data[i]), options)
		correctValue := calculatePolynomialHashByLoop(data, windowStart, i+1, options)
		if hash.Value != correctValue {
			t.Fatalf("Failed with indexes: [%d, %d), hash value: %d, correct hash value: %d",
				windowStart, i+1, hash.Value, correctValue)
		}
	}

	for ; i != SIZE; i++ {
		hash = hash.PopLeft(int32(data[windowStart]), options)
		windowStart++
		hash = hash.Append(int32(data[i]), options)
		correctValue := calculatePolynomialHashByLoop(data, windowStart, i+1, options)
		if hash.Value != correctValue {
			t.Fatalf("Failed with indexes: [%d, %d), hash value: %d, correct hash value: %d",
				windowStart, i+1, hash.Value, correctValue)
		}
	}
}