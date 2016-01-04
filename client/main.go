package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	pb "github.com/matope/grpc-kvs/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var addr = "localhost:50051"

func main() {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKvsClient(conn)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			os.Exit(0)
		}
		args := strings.Split(scanner.Text(), " ")

		switch args[0] {
		case "get":
			if len(args) < 1 {
				continue
			}
			var key = args[1]
			resp, err := c.Get(context.Background(), &pb.GetRequest{Key: key})
			if err != nil {
				log.Printf("Error:%s", err.Error())
				continue
			}
			fmt.Printf("Key:[%s] Value:[%s]\n", key, resp.Value)
		case "put":
			if len(args) < 3 {
				continue
			}
			key, value := args[1], args[2]
			_, err = c.Put(context.Background(), &pb.PutRequest{Key: key, Value: value})
			if err != nil {
				log.Fatalf("Error:%v", err)
				continue
			}
			fmt.Println("OK")
		case "delete":
			if len(args) < 1 {
				continue
			}
			var key = args[1]
			_, err := c.Delete(context.Background(), &pb.DeleteRequest{Key: key})
			if err != nil {
				log.Printf("Error:%s", err.Error())
				continue
			}
			fmt.Println("OK")
		case "range":
			startKey := ""
			if len(args) > 1 {
				startKey = args[1]
			}
			rc, err := c.Range(context.Background(), &pb.RangeRequest{StartKey: startKey})
			if err != nil {
				log.Fatalf("Error:%v", err)
				continue
			}
			for {
				resp, err := rc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Println(err.Error())
					break
				}
				fmt.Printf("Key: [%s] Value: [%s]\n", resp.Key, resp.Value)
			}
		case "watch":
			prefix := ""
			if len(args) > 1 {
				prefix = args[1]
			}
			rc, err := c.Watch(context.Background(), &pb.WatchRequest{Prefix: prefix})
			if err != nil {
				log.Fatalf("Error:%v", err)
				continue
			}
			for {
				resp, err := rc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Println(err.Error())
					break
				}
				fmt.Printf("Key: [%s] Value: [%s]\n", resp.Key, resp.Value)
			}
		default:
			log.Printf("Unknown command. cmd:[%v]", args)
		}
	}
}
