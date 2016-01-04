package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"
)

var (
	port = ":50051"
	addr = "localhost:50051"
)

func main() {
	flag.Parse()
	switch flag.Arg(0) {
	case "server":
		server()
	case "client":
		client()
	}
}

func server() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterKvsServer(s, NewKvsServer())
	s.Serve(lis)
}

func client() {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := NewKvsClient(conn)

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
			resp, err := c.Get(context.Background(), &GetRequest{Key: key})
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
			_, err = c.Put(context.Background(), &PutRequest{Key: key, Value: value})
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
			_, err := c.Delete(context.Background(), &DeleteRequest{Key: key})
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
			rc, err := c.Range(context.Background(), &RangeRequest{StartKey: startKey})
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
			rc, err := c.Watch(context.Background(), &WatchRequest{Prefix: prefix})
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

type kvsServer struct {
	elements map[string]string
	mu       sync.RWMutex
	chans    map[chan Entry]struct{}
}

func NewKvsServer() *kvsServer {
	return &kvsServer{
		elements: make(map[string]string),
		chans:    make(map[chan Entry]struct{}),
	}
}

func (s *kvsServer) Get(ctx context.Context, r *GetRequest) (*GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.elements[r.Key]; ok {
		return &GetResponse{
			Value: val,
		}, nil
	}
	return &GetResponse{}, grpc.Errorf(codes.NotFound, "element not found value=[%s]", r.Key)
}

func (s *kvsServer) Put(ctx context.Context, r *PutRequest) (*PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.elements[r.Key] = r.Value

	// Notify updation
	for c := range s.chans {
		c <- Entry{Key: r.Key, Value: r.Value}
	}
	return &PutResponse{}, nil
}

func (s *kvsServer) Delete(ctx context.Context, r *DeleteRequest) (*DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.elements, r.Key)

	// Notify deletion
	for c := range s.chans {
		c <- Entry{Key: r.Key}
	}

	return &DeleteResponse{}, nil
}

func (s *kvsServer) Range(r *RangeRequest, rs Kvs_RangeServer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// sort and filter  keys of elements
	keys := make([]string, 0, len(s.elements))
	for k := range s.elements {
		if k < r.StartKey {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if err := rs.Send(&Entry{Key: k, Value: s.elements[k]}); err != nil {
			return err
		}
	}
	return nil
}

func (s *kvsServer) Watch(r *WatchRequest, ws Kvs_WatchServer) error {
	ech := make(chan Entry)
	s.mu.Lock()
	s.chans[ech] = struct{}{}
	s.mu.Unlock()
	fmt.Println("Added New Watcher", ech)

	defer func() {
		s.mu.Lock()
		delete(s.chans, ech)
		s.mu.Unlock()
		close(ech)
		fmt.Println("Deleted Watcher", ech)
	}()

	for e := range ech {
		if !strings.HasPrefix(e.Key, r.Prefix) {
			continue
		}
		err := ws.Send(&e)
		if err != nil {
			return err
		}
	}
	return nil
}
