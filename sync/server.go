package sync

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
)

type Command struct {
	filename	string
	file		string
}

func ParseCommand(scanner *bufio.Scanner) (Command, error) {
	for scanner.Scan() {
	}

	if err := scanner.Err(); err != nil {
		return Command{}, err
	}

	text := scanner.Text()
	log.Printf("process command %s", text)

	var command, file string
	_, err := fmt.Sscanf("%s %s", text, command, file)
	if err != nil {
		return Command{}, errors.New("wrong command")
	}

	if command != "upload" {
		return Command{}, errors.New("wrong argument")
	}

	return Command{command, file}, nil
}

func handleCommand(command Command, scanner *bufio.Scanner, writer *bufio.Writer) error {

}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	command, err := ParseCommand(scanner)
	if err != nil {
		log.Printf("error: %s", err.Error())
		return
	}

	writer := bufio.NewWriter(conn)
	err = handleCommand(command, scanner, writer)
}

func StartServer(port uint16) {
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
		go handleConnection(conn)
	}
}
