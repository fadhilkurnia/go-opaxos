package paxi

import (
	"flag"
	"github.com/ailidani/paxi/log"
	"runtime"
)

var isLogStdOut = flag.Bool("log_stdout", false, "print out log in stdout instead of in the files")

// Init setup paxi package
func Init() {
	flag.Parse()
	if *isLogStdOut != true {
		log.Setup()
	}
	config.Load()
	runtime.LockOSThread()
}
