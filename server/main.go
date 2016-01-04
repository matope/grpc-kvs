package main

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"

	pb "github.com/matope/grpc-kvs/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var port = ":50051"

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKvsServer(s, NewKvsServer())
	s.Serve(lis)
}

type kvsServer struct {
	elements map[string]string
	mu       sync.RWMutex
	chans    map[chan pb.Entry]struct{}
}

func NewKvsServer() *kvsServer {
	return &kvsServer{
		elements: make(map[string]string),
		chans:    make(map[chan pb.Entry]struct{}),
	}
}

func (s *kvsServer) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.elements[r.Key]; ok {
		return &pb.GetResponse{
			Value: val,
		}, nil
	}
	return &pb.GetResponse{}, grpc.Errorf(codes.NotFound, "element not found value=[%s]", r.Key)
}

func (s *kvsServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.elements[r.Key] = r.Value

	// Notify updation
	for c := range s.chans {
		c <- pb.Entry{Key: r.Key, Value: r.Value}
	}
	return &pb.PutResponse{}, nil
}

func (s *kvsServer) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.elements, r.Key)

	// Notify deletion
	for c := range s.chans {
		c <- pb.Entry{Key: r.Key}
	}

	return &pb.DeleteResponse{}, nil
}

func (s *kvsServer) Range(r *pb.RangeRequest, rs pb.Kvs_RangeServer) error {
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
		if err := rs.Send(&pb.Entry{Key: k, Value: s.elements[k]}); err != nil {
			return err
		}
	}
	return nil
}

func (s *kvsServer) Watch(r *pb.WatchRequest, ws pb.Kvs_WatchServer) error {
	ech := make(chan pb.Entry)
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
