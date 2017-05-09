package coordinator

import (
  "hank-go-client/hank_thrift"
  "github.com/curator-go/curator"
  "hank-go-client/hank_iface"
  "hank-go-client/hank_zk"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "strings"
  "hank-go-client/hank_util"
)

type ZkHost struct {
  path string

  metadata *hank_zk.ZkWatchedNode
}


func createZkHost(ctx *hank_thrift.ThreadCtx, client curator.CuratorFramework, rootPath string, hostName string, port int, flags []string) (hank_iface.Host, error){

  metadata := hank.NewHostMetadata()
  metadata.HostName = hostName
  metadata.PortNumber = int32(port)
  metadata.Flags = strings.Join(flags,",")

  node, err := hank_zk.NewThriftZkWatchedNode(client, curator.PERSISTENT, rootPath, ctx, metadata)
  if err != nil{
    return nil, err
  }

  return &ZkHost{path: rootPath, metadata:node}, nil
}


func loadZkHost(ctx *hank_thrift.ThreadCtx, rootPath string, client curator.CuratorFramework) (interface{}, error) {

  node, err := hank_zk.LoadZkWatchedNode(client, rootPath)
  if err != nil {
    return nil, err
  }

  return &ZkHost{path: rootPath, metadata:node}, nil
}

func (p *ZkHost) GetMetadata(ctx *hank_thrift.ThreadCtx) (*hank.HostMetadata, error) {
  return hank_util.GetHostMetadata(ctx, p.metadata.Get)
}
