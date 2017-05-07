package coordinator

import (
  "github.com/curator-go/curator"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_zk"
  "path"
  "hank-go-client/hank_thrift"
)

const CLIENT_ROOT string = "c"
const CLIENT_NODE string = "c"

type ZkRingGroup struct {
  ringGroupPath string
  client        curator.CuratorFramework

  clients *hank_zk.ZkWatchedMap
}

//  loader

func loadClientMetadata(ctx *hank_thrift.ThreadCtx, path string, client curator.CuratorFramework) (interface{}, error) {
  metadata := hank.NewClientMetadata()
  hank_zk.LoadThrift(ctx, path, client, metadata)
  return metadata, nil
}

func loadZkRingGroup(ctx *hank_thrift.ThreadCtx, root string, client curator.CuratorFramework) (interface{}, error) {

  clients, err := hank_zk.NewZkWatchedMap(client, path.Join(root, CLIENT_ROOT), loadClientMetadata)

  if err != nil {
    return nil, err
  }

  return &ZkRingGroup{
    ringGroupPath: root,
    client:        client,
    clients:       clients, }, nil
}

//  methods

func (p *ZkRingGroup) RegisterClient(ctx *hank_thrift.ThreadCtx, metadata *hank.ClientMetadata) error {
  return ctx.SetThrift(hank_zk.CreateEphemeralSequential(path.Join(p.clients.Root, CLIENT_NODE), p.client), metadata)
}
