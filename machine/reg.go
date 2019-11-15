package machine

import (
	context "context"

	empty "github.com/golang/protobuf/ptypes/empty"
)

// Registrator implements "machine" API
type Registrator struct {
	Hostname string
}

// Version ...
func (reg *Registrator) Version(context.Context, *empty.Empty) (*VersionReply, error) {
	return &VersionReply{
		Response: []*VersionResponse{
			&VersionResponse{
				Metadata: &NodeMetadata{
					Hostname: reg.Hostname,
				},
				Version: &VersionInfo{
					Tag: "testme",
				},
			},
		},
	}, nil
}
