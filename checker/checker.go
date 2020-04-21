package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ailidani/paxi"
)

var file = flag.String("log", "log.csv", "")

func main() {
	flag.Parse()

	h := paxi.NewHistory()

	err := h.ReadFile(*file)
	if err != nil {
		//Fatal等价于{Print(v...); os.Exit(1)}
		log.Fatal(err)
	}

	n := h.Linearizable()

	fmt.Println(n)
}
