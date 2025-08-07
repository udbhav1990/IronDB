package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/udbhav1990/IronDB/internal/kv"
)

func handleConnection(conn net.Conn, store *kv.IronDB) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(conn, "ERR %v\n", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		args := strings.Split(line, " ")
		if len(args) == 0 {
			continue
		}

		switch strings.ToUpper(args[0]) {
		case "PUT":
			if len(args) != 3 {
				conn.Write([]byte("ERR usage: PUT key value\n"))
				continue
			}
			store.Put(args[1], args[2])
			conn.Write([]byte("OK\n"))

		case "GET":
			if len(args) != 2 {
				conn.Write([]byte("ERR usage: GET key\n"))
				continue
			}
			val, ok := store.Get(args[1])
			if ok {
				conn.Write([]byte(val + "\n"))
			} else {
				conn.Write([]byte("NOT_FOUND\n"))
			}

		case "DELETE":
			if len(args) != 2 {
				conn.Write([]byte("ERR usage: DELETE key\n"))
				continue
			}
			store.Delete(args[1])
			conn.Write([]byte("OK\n"))

		case "RANGE":
			if len(args) != 3 {
				conn.Write([]byte("ERR usage: RANGE start end\n"))
				continue
			}
			items := store.ReadKeyRange(args[1], args[2])
			for k, v := range items {
				conn.Write([]byte(fmt.Sprintf("%s:%s\n", k, v)))
			}
			conn.Write([]byte("END\n"))

		case "BATCHPUT":
			if len(args)%2 != 1 {
				conn.Write([]byte("ERR usage: BATCHPUT k1 v1 k2 v2 ...\n"))
				continue
			}
			var keys, vals []string
			for i := 1; i < len(args); i += 2 {
				keys = append(keys, args[i])
				vals = append(vals, args[i+1])
			}
			store.BatchPut(keys, vals)
			conn.Write([]byte("OK\n"))

		default:
			conn.Write([]byte("ERR unknown command\n"))
		}
	}
}
