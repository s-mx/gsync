package sync

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

type Command struct {
	filename	string
}

const (
	READ_DEADLINE = time.Second
)

func ParseCommand(reader io.Reader) (Command, error) {
	log.Print("server: try to parse command")
	scanner := bufio.NewScanner(reader)
	if !scanner.Scan() {
		return Command{}, scanner.Err()
	}

	if err := scanner.Err(); err != nil {
		return Command{}, fmt.Errorf("failed to read the line. error: %w", err)
	}

	text := scanner.Text()

	var command, file string
	_, err := fmt.Sscanf(text, "%s %s", &command, &file)
	if err != nil {
		return Command{}, fmt.Errorf("wrong line. error: %w", err)
	}

	if command != "sync" {
		return Command{}, fmt.Errorf("wrong argument: %s", command)
	}

	log.Printf("server: command has been parsed. %s", file)
	return Command{file}, nil
}

func handleConnection(conn net.Conn, filename, data string) {
	defer func () {
		_ = conn.Close()
	} ()

	destinationTransmitter := CreateDestinationTransmitter(filename, data, DestinationOptions, conn)
	if err := destinationTransmitter.PerformTransmission(context.Background()); err != nil {
		log.Printf("transmission failed. error: %v", err)
		return
	}
}

func StartServer(port uint16, filename string, data string) {
	ln, err := net.Listen("tcp", "localhost:" + strconv.Itoa(int(port)))
	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("%s", err.Error())
		}

		log.Printf("handle connection: %s", conn.RemoteAddr().String())
		go handleConnection(conn, filename, data)
	}
}
