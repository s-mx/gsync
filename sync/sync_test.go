package sync

import "testing"

func TestBuildBlockHashes(t *testing.T) {

}

func TestSimple(t *testing.T) {
	var testArgs = []struct {
		sourceData 		string
		destinationData string
	} {
		{"a", "b"},
	}


	for _, args := range testArgs {
		sourceTransmitter := CreateSourceTransmitter(args.sourceData)
		destinationTransmitter := CreateDestinationTransmitter(args.destinationData)

		destinationHashBlocks := destinationTransmitter.BuildBlockHashes()
		stream := sourceTransmitter.MatchBlockHashesAndStreamDiff(destinationHashBlocks)
		data := destinationTransmitter.ConstructOriginalData(stream)
		if data != args.sourceData {
			t.Errorf("expected: %s, got: %s", args.sourceData, data)
		}
	}
}
