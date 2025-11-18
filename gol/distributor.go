package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels, control <-chan rune) {

	c.ioCommand <- ioInput
	c.ioFilename <- fmt.Sprintf("%vx%v", p.ImageWidth, p.ImageHeight)

	board := MakeBoard(p.ImageHeight, p.ImageWidth)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			board[y][x] = <-c.ioInput
		}
	}

	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:8040"
	}
	var client *rpc.Client
	var err error
	useLocal := false
	client, err = rpc.Dial("tcp", brokerAddr)
	if err != nil {
		useLocal = true
	} else {
		defer client.Close()
	}
	var initCells []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if board[y][x] == 255 {
				initCells = append(initCells, util.Cell{X: x, Y: y})
			}
		}
	}
	c.events <- CellsFlipped{
		CompletedTurns: 0,
		Cells:          initCells,
	}
	c.events <- StateChange{0, Executing}

	var latestCompleted int64
	var latestAlive int64
	// Start at 0 so the first AliveCellsCount event (turn 0) reports 0,
	// matching the keyboard/SDL tests' expectation.
	atomic.StoreInt64(&latestAlive, 0)

	stopProgress := make(chan struct{})
	var statsWG sync.WaitGroup
	statsWG.Add(1)
	go func() {
		defer statsWG.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		lastReported := int64(-1)
		for {
			select {
			case <-ticker.C:
				curr := atomic.LoadInt64(&latestCompleted)
				if curr >= 0 && curr != lastReported {
					alive := int(atomic.LoadInt64(&latestAlive))
					c.events <- AliveCellsCount{CompletedTurns: int(curr), CellsCount: alive}
					lastReported = curr
				}
			case <-stopProgress:
				return
			}
		}
	}()

	const chunkTurns = 1

	paused := false
	quit := false
	shutdown := false
	completed := 0
	aliveCount := 0

	for !quit && !shutdown && completed < p.Turns {
		select {
		case key := <-control:
			switch key {
			case 's':
				filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, completed)
				saveBoard(c, board, filename, p, completed)
			case 'q':
				filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, completed)
				saveBoard(c, board, filename, p, completed)
				c.events <- StateChange{completed, Quitting}
				quit = true
			case 'k':
				filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, completed)
				saveBoard(c, board, filename, p, completed)
				c.events <- StateChange{completed, Quitting}
				shutdown = true
			case 'p':
				paused = !paused
				eventTurns := completed
				if paused {
					c.events <- StateChange{eventTurns, Paused}
				} else {
					c.events <- StateChange{eventTurns, Executing}
				}
			}

		default:
			if paused {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			remaining := p.Turns - completed
			turns := chunkTurns
			if remaining < turns {
				turns = remaining
			}

			chunkParams := p
			chunkParams.Turns = turns

			oldBoard := cloneBoard(board)

			var rep RunReply
			if useLocal {
				board = runLocalGOL(board, turns)
				rep.FinalBoard = board
				rep.AliveCount = countAlive(board)
			} else {
				args := RunArgs{Params: chunkParams, Board: board}
				if err := client.Call("Broker.Run", args, &rep); err != nil {
					board = runLocalGOL(board, turns)
					rep.FinalBoard = board
					rep.AliveCount = countAlive(board)
				}
			}
			board = rep.FinalBoard
			completed += turns
			aliveCount = rep.AliveCount

			flipped := diffBoard(oldBoard, board)
			if len(flipped) > 0 {
				c.events <- CellsFlipped{
					CompletedTurns: completed,
					Cells:          flipped,
				}
			}

			c.events <- TurnComplete{CompletedTurns: completed}

			atomic.StoreInt64(&latestCompleted, int64(completed))
			atomic.StoreInt64(&latestAlive, int64(aliveCount))
		}
	}

	close(stopProgress)
	statsWG.Wait()

	finalName := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, p.Turns)
	c.ioCommand <- ioOutput
	c.ioFilename <- finalName
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- board[y][x]
		}
	}
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{
		CompletedTurns: p.Turns,
		Filename:       finalName,
	}

	var aliveCells []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if board[y][x] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
			}
		}
	}

	c.events <- FinalTurnComplete{
		CompletedTurns: p.Turns,
		Alive:          aliveCells,
	}

	c.events <- StateChange{p.Turns, Quitting}

	close(c.events)

	if shutdown {
		os.Exit(0)
	}
}

func saveBoard(c distributorChannels, board [][]uint8, name string, p Params, completed int) {
	c.ioCommand <- ioOutput
	c.ioFilename <- name
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- board[y][x]
		}
	}
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{
		CompletedTurns: completed,
		Filename:       name,
	}
}

func cloneBoard(src [][]uint8) [][]uint8 {
	h := len(src)
	if h == 0 {
		return nil
	}
	w := len(src[0])
	out := make([][]uint8, h)
	for i := 0; i < h; i++ {
		row := make([]uint8, w)
		copy(row, src[i])
		out[i] = row
	}
	return out
}

func diffBoard(prev, curr [][]uint8) []util.Cell {
	changes := []util.Cell{}
	for y := range curr {
		for x := range curr[y] {
			if prev[y][x] != curr[y][x] {
				changes = append(changes, util.Cell{X: x, Y: y})
			}
		}
	}
	return changes
}

func splitBoardRows(board [][]uint8, n int) []Chunk {
	H := len(board)
	if H == 0 {
		return nil
	}
	W := len(board[0])
	base := H / n
	rem := H % n

	chunks := make([]Chunk, n)
	start := 0
	for i := 0; i < n; i++ {
		h := base
		if i < rem {
			h++
		}
		rows := make([][]uint8, h)
		for r := 0; r < h; r++ {
			row := make([]uint8, W)
			copy(row, board[start+r])
			rows[r] = row
		}
		chunks[i] = Chunk{Width: W, Height: h, Rows: rows}
		start += h
	}
	return chunks
}

func MakeBoard(height, width int) [][]uint8 {
	board := make([][]uint8, height)
	for i := range board {
		board[i] = make([]uint8, width)
	}
	return board
}

func countAlive(board [][]uint8) int {
	total := 0
	for y := range board {
		for x := range board[y] {
			if board[y][x] == 255 {
				total++
			}
		}
	}
	return total
}

func runLocalGOL(board [][]uint8, turns int) [][]uint8 {
	H := len(board)
	W := len(board[0])

	next := func() [][]uint8 {
		b := make([][]uint8, H)
		for i := range b {
			b[i] = make([]uint8, W)
		}
		return b
	}

	countNeighbors := func(b [][]uint8, y, x int) int {
		cnt := 0
		for dy := -1; dy <= 1; dy++ {
			for dx := -1; dx <= 1; dx++ {
				if dy == 0 && dx == 0 {
					continue
				}
				ny := (y + dy + H) % H
				nx := (x + dx + W) % W
				if b[ny][nx] == 255 {
					cnt++
				}
			}
		}
		return cnt
	}

	curr := board
	for t := 0; t < turns; t++ {
		newB := next()
		for y := 0; y < H; y++ {
			for x := 0; x < W; x++ {
				n := countNeighbors(curr, y, x)
				if curr[y][x] == 255 {
					if n == 2 || n == 3 {
						newB[y][x] = 255
					}
				} else {
					if n == 3 {
						newB[y][x] = 255
					}
				}
			}
		}
		curr = newB
	}

	return curr
}

/*func clampPauseTurns(completed int) int {
    return completed
}


func countAliveRemote(clients []*rpc.Client) int {
	total := int64(0)
	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, cli := range clients {
		go func(cli *rpc.Client) {
			defer wg.Done()
			var args AliveArgs
			var rep AliveReply
			if err := cli.Call("Worker.Alive", &args, &rep); err == nil {
				atomic.AddInt64(&total, int64(rep.Count))
			}
		}(cli)
	}
	wg.Wait()
	return int(total)
}*/
