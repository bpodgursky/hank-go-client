package coordinator

import (
	"github.com/curator-go/curator"
	"github.com/curator-go/curator/recipes/cache"
)

type ZkWatchedNode struct {
	node   *cache.TreeCache
	client curator.CuratorFramework
	path   string
}

func NewZkWatchedNode(client curator.CuratorFramework, path string, create bool) (r *ZkWatchedNode) {

	node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector)
	node.SetCreateParentNodes(create)
	node.SetMaxDepth(0)
	node.Start()

	return &ZkWatchedNode{node: node, client: client, path: path}
}

func (p *ZkWatchedNode) Get() ([]byte, error) {
	data, err := p.node.CurrentData(p.path);

	if err != nil {
		return nil, err
	}

	return data.Data(), nil
}

func (p *ZkWatchedNode) Set(value []byte) (error) {
	_, err := p.client.SetData().ForPathWithData(p.path, value)
	return err
}