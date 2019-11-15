package machine

import (
	"log"
	"net"

	grpc "google.golang.org/grpc"
)

// Run the service
func Run(hostname, endpoint string) {
	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	RegisterMachineServer(s, &Registrator{Hostname: hostname})
	if err := s.Serve(l); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
