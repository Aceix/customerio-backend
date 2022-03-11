package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/customerio/homework/datastore"
	"github.com/customerio/homework/serve"
	"github.com/customerio/homework/stream"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	var ds serve.Datastore

	dataSource := flag.String("data-source", "", "file get records data from")

	flag.Parse()
	if *dataSource == "" {
		*dataSource = "./data/messages.1.data"
	}

	fmt.Println("Using data source file:", *dataSource)
	file, err := os.OpenFile(*dataSource, os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal("Failed to open data file:", dataSource, err)
	}

	defer file.Close()

	if ch, err := stream.Process(ctx, file); err == nil {
		ds, err = datastore.New(ch)
		if err != nil {
			log.Fatalf("Failed to load data store: %v", err)
		}

		if err := ctx.Err(); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("stream processing failed, maybe you need to implement it?", err)
	}

	if ds == nil {
		log.Fatal("you need to implement the serve.Datastore interface to run the server")
	}

	if err := serve.ListenAndServe(":1323", ds); err != nil {
		log.Fatal(err)
	}
}
