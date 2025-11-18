package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
)

type Worker struct {
	w, h  int
	world [][]uint8
	next  [][]uint8
	torus bool
	// threads controls internal parallelism for this worker.
	threads int
}

func (w *Worker) Init(args *gol.InitArgs, rep *gol.InitReply) error {
	w.w, w.h = args.Chunk.Width, args.Chunk.Height
	w.world = clone2D(args.Chunk.Rows)
	w.next = make2D(w.h, w.w)
	w.torus = args.IsToroidal
	w.threads = runtime.NumCPU()
	if v := os.Getenv("WORKER_THREADS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			w.threads = n
		}
	}
	rep.Ok = true
	return nil
}

func (w *Worker) GetEdges(_ *gol.EdgesArgs, rep *gol.EdgesReply) error {
	top := make([]uint8, w.w)
	bot := make([]uint8, w.w)
	copy(top, w.world[0])
	copy(bot, w.world[w.h-1])
	rep.Top, rep.Bottom = top, bot
	return nil
}

func (w *Worker) Step(args *gol.StepArgs, rep *gol.StepReply) error {
	threads := w.threads
	if threads <= 0 {
		threads = 1
	}
	if threads > w.h {
		threads = w.h
	}

	rowsPer := w.h / threads
	rem := w.h % threads

	var wg sync.WaitGroup
	start := 0
	for i := 0; i < threads; i++ {
		h := rowsPer
		if i < rem {
			h++
		}
		sy, ey := start, start+h
		start = ey

		wg.Add(1)
		go func(sy, ey int) {
			defer wg.Done()
			for y := sy; y < ey; y++ {
				for x := 0; x < w.w; x++ {
					alive := 0
					for dy := -1; dy <= 1; dy++ {
						for dx := -1; dx <= 1; dx++ {
							if dy == 0 && dx == 0 {
								continue
							}
							ny := y + dy
							nx := (x + dx + w.w) % w.w
							var v uint8
							if ny < 0 {
								v = args.HaloTop[nx]
							} else if ny >= w.h {
								v = args.HaloBottom[nx]
							} else {
								v = w.world[ny][nx]
							}
							if v == 255 {
								alive++
							}
						}
					}
					cur := w.world[y][x]
					if cur == 255 {
						if alive == 2 || alive == 3 {
							w.next[y][x] = 255
						} else {
							w.next[y][x] = 0
						}
					} else {
						if alive == 3 {
							w.next[y][x] = 255
						} else {
							w.next[y][x] = 0
						}
					}
				}
			}
		}(sy, ey)
	}

	wg.Wait()
	w.world, w.next = w.next, w.world
	rep.Ok = true
	return nil
}

func (w *Worker) Get(_ *gol.GetArgs, rep *gol.GetReply) error {
	rep.Chunk.Width, rep.Chunk.Height = w.w, w.h
	rep.Chunk.Rows = clone2D(w.world)
	return nil
}

func (w *Worker) Alive(_ *gol.AliveArgs, rep *gol.AliveReply) error {
	count := 0
	for y := 0; y < w.h; y++ {
		for x := 0; x < w.w; x++ {
			if w.world[y][x] == 255 {
				count++
			}
		}
	}
	rep.Count = count
	return nil
}

func make2D(h, w int) [][]uint8 {
	m := make([][]uint8, h)
	for i := range m {
		m[i] = make([]uint8, w)
	}
	return m
}

func clone2D(src [][]uint8) [][]uint8 {
	h := len(src)
	if h == 0 {
		return nil
	}
	w := len(src[0])
	out := make([][]uint8, h)
	for i := range out {
		row := make([]uint8, w)
		copy(row, src[i])
		out[i] = row
	}
	return out
}

func main() {
	addr := ":" + getenvDefault("PORT", "8030")

	if err := rpc.Register(&Worker{}); err != nil {
		log.Fatal(err)
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}

	log.Printf("GOL Worker listening on %s", addr)
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func getenvDefault(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
