package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu      sync.Mutex
	current [][]uint8
	turns   int
}

type RunArgs struct {
	Params Params
	Board  [][]uint8
}

type RunReply struct {
	FinalBoard [][]uint8
	AliveCount int
	Turns      int
}

func (b *Broker) Run(args RunArgs, reply *RunReply) error {
	p := args.Params
	board := args.Board

	workersEnv := os.Getenv("GOL_WORKERS")
	if workersEnv == "" {
		workersEnv = "localhost:8030"
	}
	addrs := strings.Split(workersEnv, ",")

	clients := make([]*rpc.Client, len(addrs))
	failed := make([]bool, len(addrs))

	for i, addr := range addrs {
		cli, err := rpc.Dial("tcp", addr)
		if err != nil {
			failed[i] = true
			continue
		}
		clients[i] = cli
	}

	allFailed := true
	for _, ok := range failed {
		if !ok {
			allFailed = false
			break
		}
	}
	if allFailed {
		return fmt.Errorf("all workers failed to connect")
	}

	chunks := splitBoardRows(board, len(clients))

	for i, cli := range clients {
		initArgs := InitArgs{Chunk: chunks[i], IsToroidal: true}
		var rep InitReply
		err := safeCallWithTimeout(cli, "Worker.Init", &initArgs, &rep, 1*time.Second)
		if err != nil || !rep.Ok {
			fmt.Printf("Init failed on worker %d: %v\n", i, err)
			failed[i] = true
			continue
		}
	}

	completed := 0

	for completed < p.Turns {
		topRows := make([][]uint8, len(clients))
		bottomRows := make([][]uint8, len(clients))

		var wgGE sync.WaitGroup
		wgGE.Add(len(clients))
		for i, cli := range clients {
			go func(i int, cli *rpc.Client) {
				defer wgGE.Done()
				if failed[i] {
					return
				}
				var rep EdgesReply
				err := safeCallWithTimeout(cli, "Worker.GetEdges", EdgesArgs{}, &rep, 1*time.Second)
				if err != nil {
					fmt.Printf("GetEdges failed on worker %d: %v\n", i, err)
					failed[i] = true
					return
				}
				topRows[i], bottomRows[i] = rep.Top, rep.Bottom
			}(i, cli)
		}
		wgGE.Wait()

		var wgSt sync.WaitGroup
		wgSt.Add(len(clients))
		for i, cli := range clients {
			go func(i int, cli *rpc.Client) {
				defer wgSt.Done()
				if failed[i] {
					return
				}
				haloTop := bottomRows[(i-1+len(clients))%len(clients)]
				haloBottom := topRows[(i+1)%len(clients)]
				args := StepArgs{HaloTop: haloTop, HaloBottom: haloBottom}
				var rep StepReply
				err := safeCallWithTimeout(cli, "Worker.Step", args, &rep, 1*time.Second)
				if err != nil {
					fmt.Printf("Step failed on worker %d: %v\n", i, err)
					failed[i] = true
					return
				}
			}(i, cli)
		}
		wgSt.Wait()

		completed++
	}

	var finalBoard [][]uint8
	for i, cli := range clients {
		if failed[i] {
			fmt.Printf("skipping fetch from failed worker %d\n", i)
			continue
		}
		var rep GetReply
		err := safeCallWithTimeout(cli, "Worker.Get", &GetArgs{}, &rep, 1*time.Second)
		if err != nil {
			fmt.Printf("get failed on worker %d: %v\n", i, err)
			continue
		}
		finalBoard = append(finalBoard, rep.Chunk.Rows...)
	}

	reply.FinalBoard = finalBoard
	reply.AliveCount = countAlive(finalBoard)
	reply.Turns = p.Turns

	b.mu.Lock()
	b.current = finalBoard
	b.turns = p.Turns
	b.mu.Unlock()

	return nil
}

func (b *Broker) GetProgress(_ struct{}, reply *RunReply) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.current == nil {
		return fmt.Errorf("no progress")
	}
	reply.FinalBoard = b.current
	reply.AliveCount = countAlive(b.current)
	reply.Turns = b.turns
	return nil
}

func safeCallWithTimeout(cli *rpc.Client, method string, args interface{}, reply interface{}, timeout time.Duration) error {
	call := cli.Go(method, args, reply, nil)
	select {
	case <-call.Done:
		return call.Error
	case <-time.After(timeout):
		return fmt.Errorf("timeout calling %s", method)
	}
}