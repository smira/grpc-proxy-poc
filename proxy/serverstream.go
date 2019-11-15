package proxy

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ServerStreamWrapper wraps grpc.ServerStream add locking to it
type ServerStreamWrapper struct {
	stream grpc.ServerStream

	sendMu, recvMu sync.Mutex
}

// SetHeader sets the header metadata. It may be called multiple times.
// When call multiple times, all the provided metadata will be merged.
// All the metadata will be sent out when one of the following happens:
//  - ServerStream.SendHeader() is called;
//  - The first response is sent out;
//  - An RPC status is sent out (error or success).
func (wrapper *ServerStreamWrapper) SetHeader(md metadata.MD) error {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	return wrapper.stream.SetHeader(md)
}

// SendHeader sends the header metadata.
// The provided md and headers set by SetHeader() will be sent.
// It fails if called multiple times.
func (wrapper *ServerStreamWrapper) SendHeader(md metadata.MD) error {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	return wrapper.stream.SendHeader(md)
}

// SetTrailer sets the trailer metadata which will be sent with the RPC status.
// When called more than once, all the provided metadata will be merged.
func (wrapper *ServerStreamWrapper) SetTrailer(md metadata.MD) {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	wrapper.stream.SetTrailer(md)
}

// Context returns the context for this stream.
func (wrapper *ServerStreamWrapper) Context() context.Context {
	return wrapper.stream.Context()
}

// SendMsg sends a message. On error, SendMsg aborts the stream and the
// error is returned directly.
//
// SendMsg blocks until:
//   - There is sufficient flow control to schedule m with the transport, or
//   - The stream is done, or
//   - The stream breaks.
//
// SendMsg does not wait until the message is received by the client. An
// untimely stream closure may result in lost messages.
//
// It is safe to have a goroutine calling SendMsg and another goroutine
// calling RecvMsg on the same stream at the same time, but it is not safe
// to call SendMsg on the same stream in different goroutines.
func (wrapper *ServerStreamWrapper) SendMsg(m interface{}) error {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	return wrapper.stream.SendMsg(m)
}

// RecvMsg blocks until it receives a message into m or the stream is
// done. It returns io.EOF when the client has performed a CloseSend. On
// any non-EOF error, the stream is aborted and the error contains the
// RPC status.
//
// It is safe to have a goroutine calling SendMsg and another goroutine
// calling RecvMsg on the same stream at the same time, but it is not
// safe to call RecvMsg on the same stream in different goroutines.
func (wrapper *ServerStreamWrapper) RecvMsg(m interface{}) error {
	wrapper.recvMu.Lock()
	defer wrapper.recvMu.Unlock()

	return wrapper.stream.RecvMsg(m)
}
