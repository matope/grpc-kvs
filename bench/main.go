package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	pb "github.com/matope/grpc-kvs/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	addr        = "localhost:50051"
	numJobs     = 10000
	concurrency = 100
)

func main() {
	if len(os.Args) < 1 {
		log.Fatal("require mode")
	}

	switch os.Args[1] {
	case "http":
		bench_http()
	case "grpc":
		bench_grpc()
	}
}

func bench_grpc() {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKvsClient(conn)
	c.Put(context.Background(), &pb.PutRequest{Key: "key", Value: "value"})
	time.Sleep(2 * time.Second)

	wg := &sync.WaitGroup{}
	ch := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(ch chan struct{}) {
			defer wg.Done()
			for _ = range ch {

				// gRPC I/F
				_, err := c.Get(context.Background(), &pb.GetRequest{Key: "key"})
				if err != nil {
					log.Fatal(err)
				}
				//fmt.Println(resp.Value)
			}
		}(ch)
	}

	start := time.Now()
	for i := 0; i < numJobs; i++ {
		ch <- struct{}{}
	}

	close(ch)

	fmt.Printf("Waiting for workers...\n")
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("%d jobs in %0.2f seconds (%0.2f jobs/sec)\n", numJobs, float64(elapsed)/float64(time.Second), float64(numJobs)/(float64(elapsed)/float64(time.Second)))
}
