package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/ptypes/empty"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/smira/grpc-proxy-poc/machine"
	"github.com/smira/grpc-proxy-poc/proxy"
)

func Request(endpoint string) error {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := machine.NewMachineClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Version(ctx, &empty.Empty{})
	if err != nil {
		return err
	}

	spew.Dump(resp)

	return nil
}

func Director(ctx context.Context, fullMethodName string) (context.Context, []*grpc.ClientConn, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	// Copy the inbound metadata explicitly.
	outCtx, _ := context.WithCancel(ctx)
	outCtx = metadata.NewOutgoingContext(outCtx, md.Copy())
	if ok {
		conn1, err1 := grpc.DialContext(ctx, ":8051", grpc.WithCodec(proxy.Codec()), grpc.WithInsecure())
		conn2, err2 := grpc.DialContext(ctx, ":8052", grpc.WithCodec(proxy.Codec()), grpc.WithInsecure())
		var err error
		if err1 != nil {
			err = err1
		} else {
			err = err2
		}
		return outCtx, []*grpc.ClientConn{conn1, conn2}, err
	}
	return nil, nil, grpc.Errorf(codes.Unimplemented, "Unknown method")
}

func RunProxy(endpoint string) {
	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.CustomCodec(proxy.Codec()))
	// Register a TestService with 4 of its methods explicitly.
	proxy.RegisterService(server, Director,
		"machine.Machine",
		"Version")

	if err := server.Serve(l); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	go machine.Run("serverA", ":8051")
	go machine.Run("serverB", ":8052")
	go RunProxy(":8053")

	if err := Request(":8051"); err != nil {
		log.Printf("error talking to 8051: %v", err)
	}
	if err := Request(":8052"); err != nil {
		log.Printf("error talking to 8052: %v", err)
	}
	if err := Request(":8053"); err != nil {
		log.Printf("error talking to 8053: %v", err)
	}

}
