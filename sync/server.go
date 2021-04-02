package sync

import (
	"bufio"
	"context"
	"errors"
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
	scanner := bufio.NewScanner(reader)
	if !scanner.Scan() {
		return Command{}, scanner.Err()
	}

	if err := scanner.Err(); err != nil {
		log.Printf("failed to read the line. error: %v", err)
		return Command{}, err
	}

	text := scanner.Text()
	log.Printf("process command %s", text)

	var command, file string
	_, err := fmt.Sscanf(text, "%s %s", &command, &file)
	if err != nil {
		return Command{}, errors.New(fmt.Sprintf("wrong line. error: %v", err))
	}

	if command != "sync" {
		return Command{}, errors.New(fmt.Sprintf("wrong argument: %s", command))
	}

	return Command{file}, nil
}

func handleCommand(channel io.ReadWriteCloser, data string) error {
	WINDOW_LENGTH := 5
	options := DestinationTransmitterOptions{
		hashOptions: CreatePolynomialHashOptions(1234, 1000000000 + 7, WINDOW_LENGTH),
		N:           WINDOW_LENGTH,
	}

	transmitter := CreateDestinationTransmitter(data, options, channel)
	newData, err := transmitter.PerformTransmission(context.Background())
	if err != nil {
		return err
	}

	log.Printf("Got data: %s", newData)
	return nil
}

func handleConnection(conn net.Conn, filename, data string) {
	defer func () {
		_ = conn.Close()
	} ()

	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		log.Printf("couldn't set ReadDeadline. error: %v", err.Error())
		return
	}

	command, err := ParseCommand(conn)
	if err != nil {
		log.Printf("error: %s", err.Error())
		return
	}

	if command.filename != filename {
		err = errors.New(fmt.Sprintf("unknown filename: %s", filename))
		log.Printf("%v", err.Error())
		return
	}

	err = handleCommand(conn, data)
	err = conn.Close()
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
