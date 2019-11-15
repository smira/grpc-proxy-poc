// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"io"
	"log"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	clientStreamDescForProxying = &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
)

// RegisterService sets up a proxy handler for a particular gRPC service and method.
// The behaviour is the same as if you were registering a handler method, e.g. from a codegenerated pb.go file.
//
// This can *only* be used if the `server` also uses grpcproxy.CodecForServer() ServerOption.
func RegisterService(server *grpc.Server, director StreamDirector, serviceName string, methodNames ...string) {
	streamer := &handler{director}
	fakeDesc := &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*interface{})(nil),
	}
	for _, m := range methodNames {
		streamDesc := grpc.StreamDesc{
			StreamName:    m,
			Handler:       streamer.handler,
			ServerStreams: true,
			ClientStreams: true,
		}
		fakeDesc.Streams = append(fakeDesc.Streams, streamDesc)
	}
	server.RegisterService(fakeDesc, streamer)
}

// TransparentHandler returns a handler that attempts to proxy all requests that are not registered in the server.
// The indented use here is as a transparent proxy, where the server doesn't know about the services implemented by the
// backends. It should be used as a `grpc.UnknownServiceHandler`.
//
// This can *only* be used if the `server` also uses grpcproxy.CodecForServer() ServerOption.
func TransparentHandler(director StreamDirector) grpc.StreamHandler {
	streamer := &handler{director}
	return streamer.handler
}

type handler struct {
	director StreamDirector
}

// handler is where the real magic of proxying happens.
// It is invoked like any gRPC server stream and uses the gRPC server framing to get and receive bytes from the wire,
// forwarding it to a ClientStream established against the relevant ClientConn.
func (s *handler) handler(srv interface{}, serverStream grpc.ServerStream) error {
	// little bit of gRPC internals never hurt anyone
	fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return grpc.Errorf(codes.Internal, "lowLevelServerStream not exists in context")
	}
	// We require that the director's returned context inherits from the serverStream.Context().
	outgoingCtx, backendConns, err := s.director(serverStream.Context(), fullMethodName)
	if err != nil {
		return err
	}

	clientCtx, clientCancel := context.WithCancel(outgoingCtx)
	// TODO(mwitkow): Add a `forwarded` header to metadata, https://en.wikipedia.org/wiki/X-Forwarded-For.

	clientStreams := make([]grpc.ClientStream, len(backendConns))
	for i := range backendConns {
		clientStreams[i], err = grpc.NewClientStream(clientCtx, clientStreamDescForProxying, backendConns[i], fullMethodName)
		if err != nil {
			return err
		}
	}

	// Explicitly *do not close* s2cErrChan and c2sErrChan, otherwise the select below will not terminate.
	// Channels do not have to be closed, it is just a control flow mechanism, see
	// https://groups.google.com/forum/#!msg/golang-nuts/pZwdYRGxCIk/qpbHxRRPJdUJ
	s2cErrChan := s.forwardServerToClients(serverStream, clientStreams)
	c2sErrChan := s.forwardClientsToServerUnary(clientStreams, &ServerStreamWrapper{stream: serverStream})
	// We don't know which side is going to stop sending first, so we need a select between the two.
	select {
	case s2cErr := <-s2cErrChan:

		// however, we may have gotten a receive error (stream disconnected, a read error etc) in which case we need
		// to cancel the clientStream to the backend, let all of its goroutines be freed up by the CancelFunc and
		// exit with an error to the stack
		clientCancel()
		return grpc.Errorf(codes.Internal, "failed proxying s2c: %v", s2cErr)
	case c2sErr := <-c2sErrChan:
		// c2sErr will contain RPC error from client code. If not io.EOF return the RPC error as server stream error.
		if c2sErr != io.EOF {
			return c2sErr
		}
		return nil
	}
}

// unary version (need merge)
func (s *handler) forwardClientsToServerUnary(sources []grpc.ClientStream, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)

	var wg sync.WaitGroup

	payloadCh := make(chan []byte, len(sources))

	for i := 0; i < len(sources); i++ {
		wg.Add(1)
		go func(i int, src grpc.ClientStream) {
			defer wg.Done()

			f := &frame{}
			for j := 0; ; j++ {
				if err := src.RecvMsg(f); err != nil {
					if err == io.EOF {
						// This happens when the clientStream has nothing else to offer (io.EOF), returned a gRPC error. In those two
						// cases we may have received Trailers as part of the call. In case of other errors (stream closed) the trailers
						// will be nil.
						dst.SetTrailer(src.Trailer())
						return
					}
					log.Printf("error receiving from client stream: %d %v", i, err)
					return
				}
				if j == 0 {
					// This is a bit of a hack, but client to server headers are only readable after first client msg is
					// received but must be written to server stream before the first msg is flushed.
					// This is the only place to do it nicely.
					md, err := src.Header()
					if err != nil {
						log.Printf("error getting headers from client stream: %d %v", i, err)
						return
					}
					if err := dst.SetHeader(md); err != nil {
						log.Printf("error setting headers from client: %d %v", i, err)
					}
				}

				payloadCh <- f.payload
			}
		}(i, sources[i])
	}

	go func() {
		wg.Wait()
		close(payloadCh)

		var merged []byte

		for b := range payloadCh {
			merged = append(merged, b...)
		}

		ret <- dst.SendMsg(&frame{payload: merged})
	}()

	return ret
}

// streaming version (no merge)
func (s *handler) forwardClientsToServer(sources []grpc.ClientStream, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)

	var wg sync.WaitGroup

	for i := 0; i < len(sources); i++ {
		wg.Add(1)
		go func(i int, src grpc.ClientStream) {
			defer wg.Done()

			f := &frame{}
			for j := 0; ; j++ {
				if err := src.RecvMsg(f); err != nil {
					if err == io.EOF {
						// This happens when the clientStream has nothing else to offer (io.EOF), returned a gRPC error. In those two
						// cases we may have received Trailers as part of the call. In case of other errors (stream closed) the trailers
						// will be nil.
						dst.SetTrailer(src.Trailer())
						return
					}
					log.Printf("error receiving from client stream: %d %v", i, err)
					return
				}
				if j == 0 {
					// This is a bit of a hack, but client to server headers are only readable after first client msg is
					// received but must be written to server stream before the first msg is flushed.
					// This is the only place to do it nicely.
					md, err := src.Header()
					if err != nil {
						log.Printf("error getting headers from client stream: %d %v", i, err)
						return
					}
					if err := dst.SetHeader(md); err != nil {
						log.Printf("error setting headers from client: %d %v", i, err)
					}
				}
				log.Printf("moving back %v", f.payload)
				if err := dst.SendMsg(f); err != nil {
					log.Printf("error sending back to server: %d %v", i, err)
					return
				}
			}
		}(i, sources[i])
	}

	go func() {
		wg.Wait()
		ret <- nil
	}()

	return ret
}

func (s *handler) forwardServerToClients(src grpc.ServerStream, destinations []grpc.ClientStream) chan error {
	ret := make(chan error, 1)
	go func() {
		f := &frame{}
		for {
			if err := src.RecvMsg(f); err != nil {
				if err == io.EOF {
					// tell clients they should not expect more data
					for i := range destinations {
						destinations[i].CloseSend()
					}
					return
				}
				ret <- err
				return
			}

			for i := range destinations {
				if err := destinations[i].SendMsg(f); err != nil {
					log.Printf("error sending to destination %d: %v", i, err)
					break
				}
			}
		}
	}()
	return ret
}