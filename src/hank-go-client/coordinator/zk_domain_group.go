package coordinator

import (
  "path"
  "github.com/curator-go/curator"
  "hank-go-client/hank_zk"
  "hank-go-client/hank_util"
  "hank-go-client/hank_thrift"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
)

type ZkDomainGroup struct {
  name     string
  metadata *hank_zk.ZkWatchedNode
}

func createZkDomainGroup(
  ctx *hank_thrift.ThreadCtx,
  client curator.CuratorFramework,
  name string,
  rootPath string) (*ZkDomainGroup, error) {

  metadataPath := path.Join(rootPath, name)

  err := hank_zk.AssertEmpty(client, metadataPath)
  if err != nil {
    return nil, err
  }

  metadata := hank.NewDomainGroupMetadata()
  metadata.DomainVersions = make(map[int32]int32)

  bytes, err := ctx.ToBytes(metadata)

  if err != nil{
    return nil, err
  }

  node, nodeErr := hank_zk.NewZkWatchedNode(
    client,
    curator.PERSISTENT,
    metadataPath,
    bytes,
  )

  if nodeErr != nil {
    return nil, nodeErr
  }

  return &ZkDomainGroup{name: name, metadata: node}, nil
}

func loadZkDomainGroup(ctx *hank_thrift.ThreadCtx, fullPath string, client curator.CuratorFramework) (interface{}, error) {

  name := path.Base(fullPath)

  err := hank_zk.AssertExists(client, fullPath)
  if err != nil {
    return nil, err
  }

  node, nodeErr := hank_zk.LoadZkWatchedNode(client, fullPath)
  if nodeErr != nil {
    return nil, nodeErr
  }

  return &ZkDomainGroup{name: name, metadata: node}, nil
}

//  public stuff

func (p *ZkDomainGroup) GetName() string {
  return p.name
}

func (p *ZkDomainGroup) GetDomainVersions(ctx *hank_thrift.ThreadCtx) {
  hank_util.GetDomainGroupMetadata(ctx, p.metadata.Get)
}
