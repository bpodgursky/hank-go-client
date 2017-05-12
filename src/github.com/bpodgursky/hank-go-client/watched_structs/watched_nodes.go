package watched_structs

import (
  "github.com/curator-go/curator"
  "github.com/bpodgursky/hank-go-client/serializers"
  "git.apache.org/thrift.git/lib/go/thrift"
)

//  thrift

func TDeserializer(ctx *serializers.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error) {
  inst := constructor()
  err := ctx.ReadThriftBytes(raw, inst.(thrift.TStruct))
  if err != nil {
    return nil, err
  }
  return inst, nil
}

func TSerializer(ctx *serializers.ThreadCtx, val interface{}) ([]byte, error) {
  bytes, err := ctx.ToBytes(val.(thrift.TStruct))
  if err != nil {
    return nil, err
  }
  return bytes, err
}

func LoadThriftWatchedNode(client curator.CuratorFramework,
  path string,
  constructor Constructor) (*ZkWatchedNode, error) {
  return LoadZkWatchedNode(client, path, constructor, TSerializer, TDeserializer)
}

func NewThriftWatchedNode(client curator.CuratorFramework,
  mode curator.CreateMode,
  path string,
  ctx *serializers.ThreadCtx,
  constructor Constructor,
  initialValue thrift.TStruct) (*ZkWatchedNode, error) {

  bytes, err := ctx.ToBytes(initialValue)
  if err != nil {
    return nil, err
  }

  return NewZkWatchedNode(client, mode, path, bytes, constructor, TSerializer, TDeserializer)
}

//  raw bytes

//  just casting
func ByteArraySerializer(ctx *serializers.ThreadCtx, val interface{}) ([]byte, error) {
  return val.([]byte), nil
}

func ByteArrayDeserializer(ctx *serializers.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error) {
  return raw, nil
}

func LoadBytesWatchedNode(client curator.CuratorFramework, path string) (*ZkWatchedNode, error) {
  return LoadZkWatchedNode(client, path, nil, ByteArraySerializer, ByteArrayDeserializer)
}

func NewBytesWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string, initialValue []byte) (*ZkWatchedNode, error){
  return NewZkWatchedNode(client, mode, path, initialValue, nil, ByteArraySerializer, ByteArrayDeserializer)
}