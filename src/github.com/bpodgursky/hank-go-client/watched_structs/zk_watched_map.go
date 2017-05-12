package watched_structs

import (
  "github.com/curator-go/curator"
  "github.com/curator-go/curator/recipes/cache"
  "path"
  "github.com/bpodgursky/hank-go-client/serializers"
)

type Loader func(ctx *serializers.ThreadCtx, client curator.CuratorFramework, path string) (interface{}, error)

type ZkWatchedMap struct {
  Root string

  node         *cache.TreeCache
  client       curator.CuratorFramework
  loader       Loader
  internalData map[string]interface{}
}

type ChildLoader struct {
  internalData map[string]interface{}
  loader       Loader
  root         string

  ctx     *serializers.ThreadCtx
}

func (p *ChildLoader) ChildEvent(client curator.CuratorFramework, event cache.TreeCacheEvent) error {

  switch event.Type {
  case cache.TreeCacheEventNodeUpdated:
    fallthrough
  case cache.TreeCacheEventNodeAdded:
    fullChildPath := event.Data.Path()
    if IsSubdirectory(p.root, fullChildPath) {
      conditionalInsert(p.ctx, client, p.loader, p.internalData, fullChildPath)
    }
  case cache.TreeCacheEventNodeRemoved:
    fullChildPath := event.Data.Path()
    delete(p.internalData, path.Base(fullChildPath))
  }

  return nil
}
func conditionalInsert(ctx *serializers.ThreadCtx, client curator.CuratorFramework, loader Loader, internalData map[string]interface{}, fullChildPath string) {
  item, err := loader(ctx, client, fullChildPath)
  if err == nil && item != nil {
    internalData[path.Base(fullChildPath)] = item
  }
}

func NewZkWatchedMap(
  client curator.CuratorFramework,
  root string,
  loader Loader) (*ZkWatchedMap, error) {

  internalData := make(map[string]interface{})

  SafeEnsureParents(client, curator.PERSISTENT, root)

  node := cache.NewTreeCache(client, root, cache.DefaultTreeCacheSelector).
    SetMaxDepth(1).
    SetCacheData(false)

  node.Listenable().AddListener(&ChildLoader{
    internalData: internalData,
    loader:       loader,
    root:         root,
    ctx:          serializers.NewThreadCtx(),
  })

  startError := node.Start()

  if startError != nil {
    return nil, startError
  }

  initialChildren, err := client.GetChildren().ForPath(root)

  if err != nil {
    return nil, err
  }

  ctx := serializers.NewThreadCtx()
  for _, element := range initialChildren {
    conditionalInsert(ctx, client, loader, internalData, path.Join(root, element))
  }

  return &ZkWatchedMap{node: node, client: client, Root: root, loader: loader, internalData: internalData}, nil
}

//  TODO is there some equivalent to Java's map interface I can use as a reference for naming here?

//  allow direct puts so we don't have to wait for callbacks to fire
func (p *ZkWatchedMap) Put(key string, value interface{}) {
  p.internalData[key] = value
}

func (p *ZkWatchedMap) Get(key string) interface{} {
  return p.internalData[key]
}

//  TODO these methods are inefficient;  is there an equivalent to ImmutableMap?

func (p *ZkWatchedMap) KeySet() []string {

  //  TODO I really hope there's a better way to get the keySet of a map, this is horrifying
  keys := make([]string, len(p.internalData))
  i := 0
  for k := range p.internalData {
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
