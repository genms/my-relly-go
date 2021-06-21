package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/query"
	"net"
	"os"
	"strconv"
	"strings"
)

const DEFAULT_PORT int = 5646
const DEFAULT_BUFFER_POOL_SIZE int = 100

var bufmgr *buffer.BufferPoolManager
var parser *query.Parser

func main() {
	port := flag.Int("p", DEFAULT_PORT, "Port no")
	poolSize := flag.Int("l", DEFAULT_BUFFER_POOL_SIZE, "Buffer pool size")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s dbfile\n", os.Args[0])
		os.Exit(1)
	}

	bufmgr, parser = openDb(flag.Args()[0], *poolSize)

	service := fmt.Sprintf(":%d", *port)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	log.Printf("Server start\n")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		//go handleClient(conn)
		handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}()

	log.Printf("%s: Connected\n", conn.RemoteAddr())
	//conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
	var executor query.Executor
	defer func() {
		if executor != nil {
			executor.Finish(bufmgr)
		}
	}()

LOOP:
	for {
		request := make([]byte, 1024)
		readLen, err := conn.Read(request)
		if err != nil {
			log.Printf("%s: %v\n", conn.RemoteAddr(), err)
			break
		}
		if readLen == 0 {
			break
		}

		cmd := string(request)
		cmd = strings.Trim(cmd, "\x00")
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}

		cmdItems := strings.SplitN(cmd, " ", 2)
		switch cmdItems[0] {
		case "QUIT", "EXIT":
			log.Printf("%s: Disconnected\n", conn.RemoteAddr())
			break LOOP

		case "PING":
			conn.Write([]byte("PONG\n"))

		case "ECHO":
			msg := ""
			if len(cmdItems) >= 2 {
				msg = cmdItems[1]
			}
			msg += "\n"
			conn.Write([]byte(msg))

		case "FIND":
			if len(cmdItems) < 2 {
				conn.Write(errMsg("Missing query string"))
				continue
			}

			if executor != nil {
				executor.Finish(bufmgr)
			}

			plan, err := parser.Parse(cmdItems[1])
			if err != nil {
				conn.Write(errMsg(err.Error()))
				continue
			}
			executor, err = plan.Start(bufmgr)
			if err != nil {
				conn.Write(errMsg(err.Error()))
				continue
			}
			conn.Write([]byte("OK\n"))

		case "NEXT":
			if executor == nil {
				conn.Write(errMsg("Query doesn't running"))
				continue
			}

			limit := 1
			if len(cmdItems) >= 2 {
				limit, err = strconv.Atoi(cmdItems[1])
				if err != nil || limit <= 0 {
					conn.Write(errMsg("Invalid argument"))
					continue
				}
			}

			encodedRecords := [][]string{}
			eof := false
			for i := 0; i < limit; i++ {
				record, err := executor.Next(bufmgr)
				if err != nil {
					if err == query.ErrEndOfIterator {
						eof = true
						break
					} else {
						conn.Write(errMsg(err.Error()))
						continue
					}
				}

				r := []string{}
				for _, col := range record {
					r = append(r, base64.StdEncoding.EncodeToString(col))
				}
				encodedRecords = append(encodedRecords, r)
			}
			if eof {
				if len(encodedRecords) == 0 {
					executor.Finish(bufmgr)
					executor = nil

					conn.Write([]byte("END\n"))
					continue
				}
			}

			msg, err := json.Marshal(encodedRecords)
			if err != nil {
				conn.Write(errMsg("JSON marshalize error"))
				continue
			}
			msg = append([]byte("RECORDS "), msg...)
			msg = append(msg, '\n')
			conn.Write(msg)

		case "END":
			if executor == nil {
				conn.Write(errMsg("Query doesn't running"))
				continue
			}
			executor.Finish(bufmgr)
			executor = nil

			conn.Write([]byte("OK\n"))

		default:
			conn.Write(errMsg("Unknown command"))
		}
	}
}

func errMsg(msg string) []byte {
	return []byte("ERROR " + msg + "\n")
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func openDb(fileName string, poolSize int) (*buffer.BufferPoolManager, *query.Parser) {
	diskManager, err := disk.OpenDiskManager(fileName)
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(poolSize)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	parser, err := query.NewParser(bufmgr)
	if err != nil {
		panic(err)
	}

	return bufmgr, parser
}
