package coordinator

import (
  "github.com/curator-go/curator"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_zk"
  "path"
  "hank-go-client/hank_thrift"
)

type ZkRingGroup struct {
  ringGroupPath string
  client curator.CuratorFramework

  clients *hank_zk.ZkWatchedMap

}

//  loader


func LoadClientMetadata(ctx *hank_thrift.ThreadCtx, path string, client curator.CuratorFramework)(interface{}, error){

  data, err := client.GetData().ForPath(path)

  if err != nil{
    return nil, err
  }

  metadata := hank.NewClientMetadata()
  ctx.ReadThriftBytes(data, metadata)

  return metadata, nil
}

func LoadZkRingGroup(ctx *hank_thrift.ThreadCtx, root string, client curator.CuratorFramework) (interface{}, error) {

  clients, err := hank_zk.NewZkWatchedMap(client, path.Join(root, "c"), LoadClientMetadata);

  if err != nil{
    return nil, err
  }

  return &ZkRingGroup{
    ringGroupPath: root,
    client: client,
    clients: clients,}, nil
}

//  methods

func (p *ZkRingGroup) RegisterClient(metadata hank.ClientMetadata) error {

  //  TODO

  //hank_zk.NewZkWatchedNode()

  return nil


}