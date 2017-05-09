package hank_zk

import (
  "github.com/curator-go/curator"
  "github.com/curator-go/curator/recipes/cache"
  "git.apache.org/thrift.git/lib/go/thrift"
  "hank-go-client/hank_thrift"
)

type ZkWatchedNode struct {
  node   *cache.TreeCache
  client curator.CuratorFramework
  path   string
}

func LoadZkWatchedNode(client curator.CuratorFramework, path string) (*ZkWatchedNode, error){

  node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector).
    SetMaxDepth(0).
    SetCacheData(true)
  err := node.Start()

  if err != nil {
    return nil, err
  }

  return &ZkWatchedNode{node: node, client: client, path: path}, nil

}

func NewThriftZkWatchedNode(client curator.CuratorFramework,
  mode curator.CreateMode,
  path string,
  ctx *hank_thrift.ThreadCtx,
  initialValue thrift.TStruct) (*ZkWatchedNode, error) {

  bytes, err := ctx.ToBytes(initialValue)
  if err != nil {
    return nil, err
  }

 return NewZkWatchedNode(client, mode, path, bytes)

}

func NewZkWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string, data []byte) (*ZkWatchedNode, error) {
  CreateWithParents(client, mode, path, data)
  return LoadZkWatchedNode(client, path)
}

func (p *ZkWatchedNode) Get() ([]byte, error) {
  data, err := p.node.CurrentData(p.path)

  if err != nil {
    return nil, err
  }

  return data.Data(), nil
}

func (p *ZkWatchedNode) Set(value []byte) (error) {
  _, err := p.client.SetData().ForPathWithData(p.path, value)
  return err
}
