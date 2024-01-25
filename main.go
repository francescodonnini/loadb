package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"io"
	"lb/pb"
	"log"
	"net"
	"os"
	"sync"
)

type server struct {
	pb.UnimplementedMathServer
	mu      sync.Mutex
	index   int
	workers []*grpc.ClientConn
}

func (s *server) next() *grpc.ClientConn {
	s.mu.Lock()
	worker := s.workers[s.index]
	s.index++
	s.index %= len(s.workers)
	s.mu.Unlock()
	return worker
}

func (s *server) GetFactors(ctx context.Context, in *pb.IntValue) (*pb.IntList, error) {
	worker := s.next()
	client := pb.NewMathClient(worker)
	return client.GetFactors(context.Background(), in)
}

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		panic(fmt.Sprintf("Expected one argument. Got %d\n", len(args)))
	}
	config, err := parseConnections(args[0])
	if err != nil {
		panic("Expected at least one address.")
	}
	workers := make([]*grpc.ClientConn, 0)
	for _, address := range config.Workers {
		conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Cannot estabilish a connection with %s\n", address)
			continue
		}
		workers = append(workers, conn)
	}
	lis, err := net.Listen("tcp", config.Location)
	if err != nil {
		panic(fmt.Sprintf("Error: %v\n", err))
	}
	public := grpc.NewServer()
	reflection.Register(public)
	pb.RegisterMathServer(public, &server{index: 0, workers: workers})
	if err := public.Serve(lis); err != nil {
		log.Printf("Failed to serve: %v\n", err)
	}
}

type Config struct {
	Location string
	Workers  []string
}

func parseConnections(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	var config = Config{}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
