package hank_iface

import (
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_thrift"
)

type Host interface {

  GetMetadata(ctx *hank_thrift.ThreadCtx) (*hank.HostMetadata, error)

  //  stub

}
