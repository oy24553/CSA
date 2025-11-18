package gol

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It should initialise channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {

	//	TODO: Put the missing channels in here.
	ioCommand := make(chan ioCommand)
	ioIdle := make(chan bool)
	filename := make(chan string)
	ioOutput := make(chan uint8)
	ioInput := make(chan uint8)
	control := make(chan rune, 16)

	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: filename,
		output:   ioOutput,
		input:    ioInput,
	}
	go startIo(p, ioChannels)

	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: filename,
		ioOutput:   ioOutput,
		ioInput:    ioInput,
	}

	go func() {
		defer close(control)
		for key := range keyPresses {
			control <- key
			if key == 'y' || key == 'k' {
				return
			}
		}
	}()

	distributor(p, distributorChannels, control)
}

