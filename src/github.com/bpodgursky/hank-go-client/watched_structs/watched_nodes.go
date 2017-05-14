package watched_structs

import (
  "github.com/curator-go/curator"
  "github.com/bpodgursky/hank-go-client/serializers"
  "git.apache.org/thrift.git/lib/go/thrift"
  "strconv"
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

  serialized, err := TSerializer(ctx, initialValue)
  if err != nil {
    return nil, err
  }

  return NewZkWatchedNode(client, mode, path, serialized, constructor, TSerializer, TDeserializer)
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

//  int

func IntSerializer(ctx *serializers.ThreadCtx, val interface{}) ([]byte, error) {
  return []byte(strconv.Itoa(val.(int))), nil
}

func IntDeserializer(ctx *serializers.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error) {
  return strconv.Atoi(string(raw))
}

func LoadIntWatchedNode(client curator.CuratorFramework, path string) (*ZkWatchedNode, error){
  return LoadZkWatchedNode(client, path, nil, IntSerializer, IntDeserializer)
}

func NewIntWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string, initialValue int) (*ZkWatchedNode, error){
  serialized, err := IntSerializer(nil, initialValue)
  if err != nil {
    return nil, err
  }

  return NewZkWatchedNode(client, mode, path, serialized, nil, IntSerializer, IntDeserializer)
}