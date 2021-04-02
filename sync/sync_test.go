package sync

import (
	"strings"
	"testing"
)

func TestBuildBlockHashes(t *testing.T) {

}

func TestSimple(t *testing.T) {
	var testArgs = []struct {
		sourceData 		string
		destinationData string
	} {
		{"", ""},
		{"", "a"},
		{"", "aaa"},
		{"", strings.Repeat("a", 100)},
		{"a", ""},
		{"aaa", ""},
		{strings.Repeat("a", 100), ""},
		{"a", "b"},
		{"aaa", "bbb"},
		{strings.Repeat("a", 100), strings.Repeat("b", 105)},
		{strings.Repeat("a", 200), strings.Repeat("a", 300)},
	}

	param := 123456789
	mod := 1_000_000_000 + 7
	windowLength := 3
	hashOptions := CreatePolynomialHashOptions(param, mod, windowLength)

	for _, args := range testArgs {
		sourceTransmitter := CreateSourceTransmitter(args.sourceData, SourceTransmitterOptions{
			hashOptions: hashOptions,
			N:           windowLength,
		})
		destinationTransmitter := CreateDestinationTransmitter(args.destinationData, DestinationTransmitterOptions{
			hashOptions: hashOptions,
			N:           windowLength,
		})

		destinationHashBlocks := destinationTransmitter.GenerateBlockHashes()

		stream := sourceTransmitter.MatchBlockHashesAndStreamDiff(destinationHashBlocks)
		data := destinationTransmitter.ConstructOriginalData(stream)
		if data != args.sourceData {
			t.Errorf("expected: %s, got: %s", args.sourceData, data)
		}
	}
}
