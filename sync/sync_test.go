package sync

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

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
		_ = server.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
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

func TestSimple(t *testing.T) {
	filename := "filename.txt"
	var testArgs = []struct {
		sourceData 		string
		destinationData string
	} {
		//{"", ""},
		//{"", "a"},
		//{"", "aaa"},
		//{"", strings.Repeat("a", 100)},
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
		client, server := net.Pipe()

		sourceOptions := SourceTransmitterOptions{
			hashOptions: hashOptions,
			N:           windowLength,
			Timeout:     time.Millisecond * 100,
		}
		sourceTransmitter := CreateSourceTransmitter(filename, args.sourceData, sourceOptions, client)

		destinationOptions := DestinationTransmitterOptions{
			hashOptions: hashOptions,
			N:           windowLength,
			Timeout:	 time.Millisecond * 100,
		}

		destinationTransmitter := CreateDestinationTransmitter(filename, args.destinationData, destinationOptions, server)

		wg := sync.WaitGroup{}
		wg.Add(2)
		var sourceError, serverError error
		var syncedData string
		go func (source *SourceTransmitter, err *error) {
			defer wg.Done()
			*err = source.PerformTransmission(context.Background())
		} (sourceTransmitter, &sourceError)

		go func (destination *DestinationTransmitter, err *error, result *string) {
			defer wg.Done()
			*err = destination.PerformTransmission(context.Background())
			*result = string(destination.newData)
		} (destinationTransmitter, &serverError, &syncedData)

		wg.Wait()

		if sourceError != nil {
			t.Errorf("client failed: %v", sourceError)
		}

		if serverError != nil {
			t.Errorf("server failed: %v", serverError)
		}

		if syncedData != args.sourceData {
			t.Errorf("expected: %s, got: %s", args.sourceData, syncedData)
		} else {
			log.Printf("Test passed")
		}
	}
}
