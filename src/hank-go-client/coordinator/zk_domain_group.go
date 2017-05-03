package coordinator

import (
  "path"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "github.com/curator-go/curator"
  "git.apache.org/thrift.git/lib/go/thrift"
)

type ZkDomainGroup struct {
  name     string
  metadata *ZkWatchedNode
}

func CreateZkDomainGroup(client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error) {
  metadataPath := path.Join(rootPath, name)

  err := assertEmpty(client, metadataPath)
  if err != nil {
    return nil, err
  }

  node := NewZkWatchedNode(client, metadataPath, true)

  metadata := hank.NewDomainGroupMetadata()

  //  TODO threadlocal or something.  ZkWatchedThriftNode
  t := thrift.NewTSerializer()
  t.Protocol = thrift.NewTCompactProtocol(t.Transport)
  data, _ := t.Write(metadata)
  node.Set(data)
  t.Transport.Close()

  return &ZkDomainGroup{name: name, metadata:node}, nil
}

func LoadZkDomainGroup(client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error){

  metadataPath := path.Join(rootPath, name)

  err := assertExists(client, metadataPath)
  if err != nil {
    return nil, err
  }

  node := NewZkWatchedNode(client, metadataPath, false)
  return &ZkDomainGroup{name: name, metadata:node}, nil
}

type ZkDomainGroupLoader struct{}

func (p *ZkDomainGroupLoader) load(fullPath string, client curator.CuratorFramework) (interface{}, error) {
  return LoadZkDomainGroup(client, path.Base(fullPath), path.Dir(fullPath))
}

func (p *ZkDomainGroup) getName() string {
  return p.name
}
