package hank_iface

import (
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_thrift"
)

type RingGroup interface {

  GetName() string

  GetRings() []Ring

  AddRing(ringNum int) Ring

  RegisterClient(ctx *hank_thrift.ThreadCtx, metadata hank.ClientMetadata) error

  //	stub
}
