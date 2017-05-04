package hank_zk

import (
	"github.com/curator-go/curator"
	"github.com/curator-go/curator/recipes/cache"
	"fmt"
)

type ZkWatchedNode struct {
	node   *cache.TreeCache
	client curator.CuratorFramework
	path   string
}

func NewZkWatchedNode(client curator.CuratorFramework, path string) (r *ZkWatchedNode) {

	SafeEnsureParents(client, path)

  node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector)
	err := node.Start()

	fmt.Println(err)

	return &ZkWatchedNode{node: node, client: client, path: path}
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
