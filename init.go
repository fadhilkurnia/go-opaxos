package paxi

import (
	"flag"
	"github.com/ailidani/paxi/log"
)

var IsLogStdOut = flag.Bool("log_stdout", false, "print out log in stdout instead of in the files")

// Init setup paxi package
func Init() {
	flag.Parse()
	if *IsLogStdOut != true {
		log.Setup()
	}
	config.Load()
}
