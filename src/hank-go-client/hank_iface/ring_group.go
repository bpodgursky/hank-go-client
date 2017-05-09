package hank_iface

import (
  "hank-go-client/hank_thrift"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
)

type RingGroup interface {
  GetName() string

  GetRings() []Ring

  AddRing(ctx *hank_thrift.ThreadCtx, ringNum int) (Ring, error)

  GetRing(ringNum int) Ring

  RegisterClient(ctx *hank_thrift.ThreadCtx, metadata *hank.ClientMetadata) error

  GetClients() []*hank.ClientMetadata

  //	stub
}
