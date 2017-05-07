package hank_util

import (
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_thrift"
  "hank-go-client/hank_iface"
)

//  suuuuure
type Getter func(name string) interface{}

//  this file is the horrifying result of not having generics, as far as I can tell.  is there any way I can avoid
//  declaring this for every single thrift type I want?  maybe we can template and autogenerate it?

// watched node cast copypasta

func GetDomainGroupMetadata(ctx *hank_thrift.ThreadCtx, get hank_thrift.GetBytes) (*hank.DomainGroupMetadata, error) {
  metadata := hank.NewDomainGroupMetadata()
  error := ctx.ReadThrift(get, metadata)
  if error != nil {
    return nil, error
  }
  return metadata, nil
}

//  watched thrift map cast copypasta

func GetDomainGroup(name string, get Getter) hank_iface.DomainGroup {
  raw := get(name)
  original, ok := raw.(hank_iface.DomainGroup)
  if ok {
    return original
  }
  return nil
}

func GetRingGroup(name string, get Getter) hank_iface.RingGroup {
  raw := get(name)
  original, ok := raw.(hank_iface.RingGroup)
  if ok {
    return original
  }
  return nil
}

func GetClientMetadata(name string, get Getter) *hank.ClientMetadata {
  raw := get(name)
  original, ok := raw.(hank.ClientMetadata)
  if ok {
    return &original
  }
  return nil
}
