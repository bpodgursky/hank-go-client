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
    metadataPath,
  )

  return &ZkDomainGroup{name: name, metadata: node}, nil
}

func LoadZkDomainGroup(client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error) {

  metadataPath := path.Join(rootPath, name)

  err := hank_zk.AssertExists(client, metadataPath)
  if err != nil {
    return nil, err
  }

  node := hank_zk.NewZkWatchedNode(client, metadataPath)
  return &ZkDomainGroup{name: name, metadata: node}, nil
}

type ZkDomainGroupLoader struct{}

func (p *ZkDomainGroupLoader) load(fullPath string, client curator.CuratorFramework) (interface{}, error) {
  return LoadZkDomainGroup(client, path.Base(fullPath), path.Dir(fullPath))
}


//  public stuff

func (p *ZkDomainGroup) GetName() string {
  return p.name
}

func (p *ZkDomainGroup) GetDomainVersions(ctx *hank_thrift.ThreadCtx) {
  hank_util.GetDomainGroupMetadata(ctx, p.metadata)
}
