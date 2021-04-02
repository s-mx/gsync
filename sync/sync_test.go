package sync

import (
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func TestBuildBlockHashes(t *testing.T) {

}

func TestSimple(t *testing.T) {
	/*
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
*/

	/*
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

	*/
}

func TestServerParseCommand(t *testing.T) {
	args := []struct{
		input, answer 	string
		success 		bool
	} {
		{"", "", false},
		{"a", "", false},
		{"123456", "", false},
		{"filename.txt", "", false},
		{"123\n", "", false},
		{"filename.txt\n", "", false},
		{"s s", "", false},
		{"s s\n", "", false},
		{"sync\n", "", false},
		{"sync aaa", "", false},
		{"sync \n", "", false},
		{"sync filename.txt\n", "filename.txt", true},
	}

	for _, arg := range args {
		log.Printf("Start test with input: %s", arg.input)
		client, server := net.Pipe()

		var command Command
		var commandError error
		wg := sync.WaitGroup{}

		wg.Add(1)
		_ = server.SetReadDeadline(time.Now().Add(time.Millisecond * 500))
		go func(reader io.Reader, command *Command, err *error, wg *sync.WaitGroup) {
			defer wg.Done()
			*command, *err = ParseCommand(reader)
		} (server, &command, &commandError, &wg)

		if _, err := io.WriteString(client, arg.input); err != nil {
			t.Fatalf("can't write input. error: %v", err)
		}

		wg.Wait()
		if arg.success && commandError != nil {
			t.Errorf("ParseCommand failed. error: %v", commandError)
		}

		if command.filename != arg.answer {
			t.Errorf("got wrong command. got: %s, expected: %s", command.filename, arg.answer)
		}
	}
}