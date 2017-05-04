package hank_thrift

import "git.apache.org/thrift.git/lib/go/thrift"

type ThreadCtx struct {
  Serializer   *thrift.TSerializer
  Deserializer *thrift.TDeserializer
}

func NewThreadCtx() *ThreadCtx {

  serializer := thrift.NewTSerializer()
  serializer.Protocol = thrift.NewTCompactProtocol(serializer.Transport)

  deserializer := thrift.NewTDeserializer()
  deserializer.Protocol = thrift.NewTCompactProtocol(deserializer.Transport)

  return &ThreadCtx{
    Serializer:   serializer,
    Deserializer: deserializer,
  }

}

func ReadThrift(ctx *ThreadCtx, node WatchedNode, emptyStruct thrift.TStruct) error {

  data, error := node.Get()
  if error != nil {
    return error
  }

  deserErr := ctx.Deserializer.Read(emptyStruct, data)
  if deserErr != nil {
    return error
  }

  return nil
}

func SetThrift(ctx *ThreadCtx, node WatchedNode, tStruct thrift.TStruct) error {

  bytes, err := ctx.Serializer.Write(tStruct)
  if err != nil {
    return err
  }

  return node.Set(bytes)
}

type WatchedNode interface {
  Get() ([]byte, error)
  Set(value []byte) (error)
}
