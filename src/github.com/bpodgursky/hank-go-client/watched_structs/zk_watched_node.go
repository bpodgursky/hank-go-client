package watched_structs

import (
	"errors"
	"fmt"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/cenkalti/backoff"
	"github.com/curator-go/curator"
	"github.com/curator-go/curator/recipes/cache"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type Constructor func() interface{}

type Deserializer func(ctx *serializers.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error)
type Serializer func(ctx *serializers.ThreadCtx, val interface{}) ([]byte, error)

type ZkWatchedNode struct {
	node        *cache.TreeCache
	client      curator.CuratorFramework
	path        string
	constructor Constructor
	ctx         *serializers.ThreadCtx

	serializer   Serializer
	deserializer Deserializer

	value interface{}
	stat  *zk.Stat
}

type ObjLoader struct {
	watchedNode *ZkWatchedNode
}

func (p *ObjLoader) ChildEvent(client curator.CuratorFramework, event cache.TreeCacheEvent) error {

	switch event.Type {
	case cache.TreeCacheEventNodeUpdated:
		fallthrough

	case cache.TreeCacheEventNodeAdded:
		data := event.Data

		obj, err := p.watchedNode.deserializer(p.watchedNode.ctx, data.Data(), p.watchedNode.constructor)
		if err != nil {
			fmt.Println(err)
			return err
		}

		p.watchedNode.value = obj
		p.watchedNode.stat = data.Stat()

	case cache.TreeCacheEventNodeRemoved:
		p.watchedNode.value = nil
		p.watchedNode.stat = &zk.Stat{}
	}

	return nil
}

//  generic

func NewZkWatchedNode(
	client curator.CuratorFramework,
	mode curator.CreateMode,
	path string,
	data []byte,
	constuctor Constructor,
	serializer Serializer,
	deserializer Deserializer) (*ZkWatchedNode, error) {

	err := CreateWithParents(client, mode, path, data)

	if err != nil {
		return nil, err
	}

	return LoadZkWatchedNode(client, path, constuctor, serializer, deserializer)
}

func LoadZkWatchedNode(client curator.CuratorFramework, path string, constructor Constructor, serializer Serializer, deserializer Deserializer) (*ZkWatchedNode, error) {

	//  TODO we might need a pool of these -- evaluate in production.  in a more civilized world, we'd just use a ThreadLocal
	ctx := serializers.NewThreadCtx()

	watchedNode := &ZkWatchedNode{client: client, path: path, constructor: constructor, ctx: ctx, serializer: serializer, deserializer: deserializer}

	node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector).
		SetMaxDepth(0).
		SetCacheData(false)

	node.Listenable().AddListener(&ObjLoader{watchedNode})
	err := node.Start()
	if err != nil {
		return nil, err
	}

	watchedNode.node = node

	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 10

	err = backoff.Retry(func() error {
		res := watchedNode.value != nil
		if !res {
			return errors.New("Node does not exist yet")
		}
		return nil
	}, backoffStrat)

	if err != nil {
		return nil, errors.New("Never found data for node path " + path)
	}

	return watchedNode, nil
}

func (p *ZkWatchedNode) Get() interface{} {
	return p.value
}

func (p *ZkWatchedNode) Set(ctx *serializers.ThreadCtx,
	value interface{}) error {

	bytes, err := p.serializer(ctx, value)
	if err != nil {
		return err
	}

	_, err = p.client.SetData().ForPathWithData(p.path, bytes)
	return err
}

// Note: update() should not modify its argument
type Updater func(interface{}) interface{}

func (p *ZkWatchedNode) Update(ctx *serializers.ThreadCtx, updater Updater) {

	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 10

	backoff.Retry(func() error {

		newValue := updater(p.value)

		bytes, err := p.serializer(ctx, newValue)
		if err != nil {
			return err
		}

		_, err = p.client.SetData().WithVersion(p.stat.Version).ForPathWithData(p.path, bytes)
		if err != nil {
			return err
		}

		return nil

	}, backoffStrat)

}
