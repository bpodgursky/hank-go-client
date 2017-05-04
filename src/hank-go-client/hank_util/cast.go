package hank_util

import (
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_thrift"
  "hank-go-client/hank_iface"
)

//  this file is the horrifying result of not having generics, as far as I can tell.  is there any way I can avoid
//  declaring this for every single thrift type I want?  maybe we can template and autogenerate it?

// watched node cast copypasta

func GetDomainGroupMetadata(ctx *hank_thrift.ThreadCtx, node hank_thrift.WatchedNode) (*hank.DomainGroupMetadata, error) {
  metadata := hank.NewDomainGroupMetadata()
  error := hank_thrift.ReadThrift(ctx, node, metadata)
  if error != nil {
    return nil, error
  }
  return metadata, nil
}

//  watched thrift map cast copypasta

func GetDomainGroup(name string, get func(name string) interface{}) hank_iface.DomainGroup {
  raw := get(name)
  original, ok := raw.(hank_iface.DomainGroup)
  if ok {
    return original
  }
  return nil
}

func GetRingGroup(name string, get func(name string) interface{}) hank_iface.RingGroup {
  raw := get(name)
  original, ok := raw.(hank_iface.RingGroup)
  if ok {
    return original
  }
  return nil
}
