package coordinator

import (
  "github.com/curator-go/curator/recipes/cache"
  "github.com/curator-go/curator"
  "path"
  "github.com/liveramp/hank-go-client/util"
)

type MapLoader interface {
  load(path string, client curator.CuratorFramework) interface{}
}

type ZkWatchedMap struct {
  node         *cache.TreeCache
  client       curator.CuratorFramework
  path         string
  loader       MapLoader
  internalData map[string]interface{}
}

type ChildLoader struct {
  internalData map[string]interface{}
  loader       MapLoader
  root         string
}

func NewChildLoader(internalData map[string]interface{}, loader MapLoader, root string) *ChildLoader {
  return &ChildLoader{internalData: internalData, loader: loader, root: root}
}

func (p *ChildLoader) ChildEvent(client curator.CuratorFramework, event cache.TreeCacheEvent) error {

  switch event.Type {

  case cache.TreeCacheEventNodeUpdated:
    fallthrough
  case cache.TreeCacheEventNodeAdded:

    fullChildPath := event.Data.Path()

    if util.IsSubdirectory(p.root, fullChildPath) {
      p.internalData[path.Base(fullChildPath)] = p.loader.load(fullChildPath, client)
    }

  case cache.TreeCacheEventNodeRemoved:

    fullChildPath := event.Data.Path()
    delete(p.internalData, path.Base(fullChildPath))

  }

  return nil
}

func NewZkWatchedMap(
  client curator.CuratorFramework,
  path string,
  loader MapLoader) (*ZkWatchedMap) {

  internalData := make(map[string]interface{})

  node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector)
  node.SetCreateParentNodes(true)
  node.SetMaxDepth(1)
  node.SetCacheData(false)
  node.Listenable().AddListener(NewChildLoader(internalData, loader, path))
  node.Start()

  return &ZkWatchedMap{node: node, client: client, path: path, loader: loader, internalData: internalData}
}

//  TODO is there some equivalent to Java's map interface I can use as a reference for naming here?

//  allow direct puts so we don't have to wait for callbacks to fire
func (p *ZkWatchedMap) Put(key string, value interface{}) {
  p.internalData[key] = value
}

func (p *ZkWatchedMap) Get(key string) (interface{}) {
  return p.internalData[key]
}

//  TODO these methods are inefficient;  is there an equivalent to ImmutableMap?

func (p *ZkWatchedMap) KeySet() []string {

  //  TODO I really hope there's a better way to get the keySet of a map, this is horrifying
  keys := make([]string, len(p.internalData))
  i := 0
  for k, _ := range p.internalData {
    keys[i] = k
    i++
  }

  return keys
}

func (p *ZkWatchedMap) Values() []interface{} {

  values := make([]interface{}, len(p.internalData))
  i := 0
  for k := range p.internalData {
    values[i] = p.internalData[k]
    i++
  }

  return values
}
