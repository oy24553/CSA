package gol

type Chunk struct {
	Width  int
	Height int
	Rows   [][]uint8 // Height,Width
}

// rpc

type InitArgs struct {
	Chunk      Chunk
	IsToroidal bool
}
type InitReply struct{ Ok bool }

type EdgesArgs struct{}
type EdgesReply struct {
	Top    []uint8 // len=Width
	Bottom []uint8 // len=Width
}

type StepArgs struct {
	HaloTop    []uint8 // len=Width
	HaloBottom []uint8 // len=Width
}
type StepReply struct{ Ok bool }

type GetArgs struct{}
type GetReply struct { Chunk Chunk }

type AliveArgs struct{}
type AliveReply struct{ Count int }

