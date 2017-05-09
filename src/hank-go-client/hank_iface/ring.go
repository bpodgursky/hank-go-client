package hank_iface

import "hank-go-client/hank_thrift"

type Ring interface {
  //  stub

  AddHost(ctx *hank_thrift.ThreadCtx, hostName string, port int, hostFlags []string) (Host, error)

  GetHosts(ctx *hank_thrift.ThreadCtx) []Host

}
