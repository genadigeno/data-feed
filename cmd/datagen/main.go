// Command datagen is the synthetic data generation platform: Coordinator, Worker and
// CLI in one binary.
package main

import (
	"os"

	"github.com/genadigeno/data-feed/internal/cli"
)

func main() {
	os.Exit(cli.Execute(os.Args[1:], os.Stdout, os.Stderr))
}
