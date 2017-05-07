package hank_zk

import (
  "github.com/curator-go/curator"
  "github.com/curator-go/curator/recipes/cache"
)

type ZkWatchedNode struct {
  node   *cache.TreeCache
  client curator.CuratorFramework
  path   string
}

func NewZkWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string) (*ZkWatchedNode, error) {

  SafeEnsureParents(client, mode, path)

  node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector).
    SetMaxDepth(0).
    SetCacheData(true)
  err := node.Start()

  if err != nil {
    return nil, err
  }

  return &ZkWatchedNode{node: node, client: client, path: path}, nil
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
