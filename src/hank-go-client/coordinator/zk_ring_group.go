package coordinator

import (
  "github.com/curator-go/curator"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_zk"
  "path"
  "hank-go-client/hank_thrift"
  "hank-go-client/hank_iface"
  "strconv"
  "hank-go-client/hank_util"
)

const CLIENT_ROOT string = "c"
const CLIENT_NODE string = "c"

type ZkRingGroup struct {
  ringGroupPath string
  name          string
  client        curator.CuratorFramework

  clients *hank_zk.ZkWatchedMap
  rings   *hank_zk.ZkWatchedMap
}

func createZkRingGroup(ctx *hank_thrift.ThreadCtx, client curator.CuratorFramework, name string, rootPath string) (hank_iface.RingGroup, error) {
  rgRootPath := path.Join(rootPath, name)

  err := hank_zk.AssertEmpty(client, rgRootPath)
  if err != nil {
    return nil, err
  }

  hank_zk.CreateWithParents(client, curator.PERSISTENT, rgRootPath, nil)

  clients, err := hank_zk.NewZkWatchedMap(client, path.Join(rgRootPath, CLIENT_ROOT), loadClientMetadata)
  if err != nil {
    return nil, err
  }

  rings, err := hank_zk.NewZkWatchedMap(client, rgRootPath, loadZkRing)
  if err != nil{
    return nil, err
  }

  return &ZkRingGroup{ringGroupPath: rootPath, name: name, client: client, clients: clients, rings: rings}, nil

}

func loadZkRingGroup(ctx *hank_thrift.ThreadCtx, rgRootPath string, client curator.CuratorFramework) (interface{}, error) {

  err := hank_zk.AssertExists(client, rgRootPath)
  if err != nil {
    return nil, err
  }

  clients, err := hank_zk.NewZkWatchedMap(client, path.Join(rgRootPath, CLIENT_ROOT), loadClientMetadata)
  if err != nil {
    return nil, err
  }

  rings, err := hank_zk.NewZkWatchedMap(client, rgRootPath, loadZkRing)

  return &ZkRingGroup{ringGroupPath: rgRootPath, client: client, clients: clients, rings: rings}, nil
}

//  loader

func loadClientMetadata(ctx *hank_thrift.ThreadCtx, path string, client curator.CuratorFramework) (interface{}, error) {
  metadata := hank.NewClientMetadata()
  hank_zk.LoadThrift(ctx, path, client, metadata)
  return metadata, nil
}

//  methods

func (p *ZkRingGroup) RegisterClient(ctx *hank_thrift.ThreadCtx, metadata *hank.ClientMetadata) error {
  return ctx.SetThrift(hank_zk.CreateEphemeralSequential(path.Join(p.clients.Root, CLIENT_NODE), p.client), metadata)
}

func (p *ZkRingGroup) GetName() string {
  return p.name
}

func (p *ZkRingGroup) GetClients() []*hank.ClientMetadata {

  groups := []*hank.ClientMetadata{}
  for _, item := range p.clients.Values() {
    i := item.(*hank.ClientMetadata)
    groups = append(groups, i)
  }

  return groups
}

func ringName(ringNum int) string {
  return "ring-" + strconv.Itoa(ringNum)
}

func (p *ZkRingGroup) AddRing(ctx *hank_thrift.ThreadCtx, ringNum int) (hank_iface.Ring, error) {
  ringChild := ringName(ringNum)
  ringRoot := path.Join(p.rings.Root, ringChild)

  ring, err := createZkRing(ctx, ringRoot, ringNum, p.client)
  if err != nil {
    return nil, err
  }

  p.rings.Put(ringChild, ring)
  return ring, nil
}

func (p *ZkRingGroup) GetRing(ringNum int) hank_iface.Ring {
  return hank_util.GetRing(ringName(ringNum), p.rings.Get)
}

func (p *ZkRingGroup) GetRings() []hank_iface.Ring {

  rings := []hank_iface.Ring{}
  for _, item := range p.rings.Values() {
    i := item.(hank_iface.Ring)
    rings = append(rings, i)
  }

  return rings
}