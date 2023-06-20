package main

import (
	"fmt"
	"os"
)

func main() {
	os.Setenv("GOGC", "50")
	fmt.Println(os.Getenv("GOGC"))
}
