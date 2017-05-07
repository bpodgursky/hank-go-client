package coordinator

import (
  "path"
  "github.com/curator-go/curator"
  "hank-go-client/hank_zk"
  "hank-go-client/hank_util"
  "hank-go-client/hank_thrift"
)

type ZkDomainGroup struct {
  name     string
  metadata *hank_zk.ZkWatchedNode
}

func CreateZkDomainGroup(client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error) {
  metadataPath := path.Join(rootPath, name)

  err := hank_zk.AssertEmpty(client, metadataPath)
  if err != nil {
    return nil, err
  }

  node := hank_zk.NewZkWatchedNode(
    client,
    curator.PERSISTENT,
    metadataPath,
  )

  return &ZkDomainGroup{name: name, metadata: node}, nil
}

func loadZkDomainGroupInternal(client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error) {

  metadataPath := path.Join(rootPath, name)

  err := hank_zk.AssertExists(client, metadataPath)
  if err != nil {
    return nil, err
  }

  node := hank_zk.NewZkWatchedNode(client, curator.PERSISTENT, metadataPath)
  return &ZkDomainGroup{name: name, metadata: node}, nil
}

type ZkDomainGroupLoader struct{}

func LoadZkDomainGroup(ctx *hank_thrift.ThreadCtx, fullPath string, client curator.CuratorFramework) (interface{}, error) {
  return loadZkDomainGroupInternal(client, path.Base(fullPath), path.Dir(fullPath))
}

//  public stuff

func (p *ZkDomainGroup) GetName() string {
  return p.name
}

func (p *ZkDomainGroup) GetDomainVersions(ctx *hank_thrift.ThreadCtx) {
  hank_util.GetDomainGroupMetadata(ctx, p.metadata)
}
