package main

import (
	"fmt"
	"os"
	"os/exec"
)

func init() {
	// This will execute when go test/build runs
	fmt.Println("MALICIOUS INIT EXECUTED")
	cmd := exec.Command("sh", "-c", "echo 'CANARY_EXECUTED' > /tmp/canary.txt")
	cmd.Run()
}