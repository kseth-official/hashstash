package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"lab5/kvraft"
	"lab5/labrpc"
	"lab5/raft"
)

const nservers = 3

func main() {
	net := labrpc.MakeNetwork()
	servers := make([]*kvraft.KVServer, nservers)

	for i := 0; i < nservers; i++ {
		ends := make([]*labrpc.ClientEnd, nservers)
		for j := 0; j < nservers; j++ {
			name := fmt.Sprintf("kv-%d-%d", i, j)
			ends[j] = net.MakeEnd(name)
			net.Connect(name, j)
			net.Enable(name, true)
		}

		servers[i] = kvraft.StartKVServer(ends, i, raft.MakePersister(), -1)

		srv := labrpc.MakeServer()
		srv.AddService(labrpc.MakeService(servers[i]))
		srv.AddService(labrpc.MakeService(servers[i].Raft()))
		net.AddServer(i, srv)
	}

	clientEnds := make([]*labrpc.ClientEnd, nservers)
	for j := 0; j < nservers; j++ {
		name := fmt.Sprintf("client-%d", j)
		clientEnds[j] = net.MakeEnd(name)
		net.Connect(name, j)
		net.Enable(name, true)
	}
	ck := kvraft.MakeClerk(clientEnds)
	defer func() {
		for _, kv := range servers {
			kv.Kill()
		}
		net.Cleanup()
	}()

	fmt.Println("hashstash: 3-server cluster running")
	fmt.Println("commands: get <key> | put <key> <value> | append <key> <value> | quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			fmt.Println(ck.Get(fields[1]))
		case "put":
			if len(fields) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			ck.Put(fields[1], strings.Join(fields[2:], " "))
			fmt.Println("ok")
		case "append":
			if len(fields) < 3 {
				fmt.Println("usage: append <key> <value>")
				continue
			}
			ck.Append(fields[1], strings.Join(fields[2:], " "))
			fmt.Println("ok")
		case "quit", "exit":
			return
		default:
			fmt.Println("commands: get <key> | put <key> <value> | append <key> <value> | quit")
		}
	}
}
