package coordinator

import (
  "github.com/curator-go/curator"
  "path"
  "github.com/bpodgursky/hank-go-client/watched_structs"
  "github.com/bpodgursky/hank-go-client/serializers"
  "github.com/bpodgursky/hank-go-client/iface"
  "github.com/bpodgursky/hank-go-client/hank_types"
  "strings"
)

type ZkDomain struct {
  name string

  metadata *watched_structs.ZkWatchedNode

  //  temp
  partitioner iface.Partitioner
}

func createZkDomain(ctx *serializers.ThreadCtx,
  root string,
  name string,
  id iface.DomainID,
  numPartitions int32,
  storageEngineFactoryName string,
  storageEngineOptions string,
  partitionerName string,
  requiredHostFlags []string,
  client curator.CuratorFramework) (*ZkDomain, error) {

  metadata := hank.NewDomainMetadata()
  metadata.ID = int32(id)
  metadata.NumPartitions = numPartitions
  metadata.StorageEngineFactoryClass = storageEngineFactoryName
  metadata.StorageEngineOptions = storageEngineOptions
  metadata.PartitionerClass = partitionerName
  metadata.RequiredHostFlags = strings.Join(requiredHostFlags, ",")

  //  TODO other metadata

  node, nodeErr := watched_structs.NewThriftWatchedNode(
    client,
    curator.PERSISTENT,
    root,
    ctx,
    iface.NewDomainMetadata,
    metadata,
  )
  if nodeErr != nil {
    return nil, nodeErr
  }

  return &ZkDomain{name: name, metadata: node}, nil

}


func (p *ZkDomain) GetPartitioner() iface.Partitioner{

  if p.partitioner == nil {
    class := iface.AsDomainMetadata(p.metadata.Get()).PartitionerClass

    //  gross, but otherwise there's no good way to get java classes to line up with Go impls
    if class == "com.liveramp.hank.partitioner.Murmur64Partitioner" {
      p.partitioner = &Murmur64Partitioner{}
    }

  }

  return p.partitioner
}


func loadZkDomain(ctx *serializers.ThreadCtx, client curator.CuratorFramework, listener serializers.DataChangeNotifier, root string) (interface{}, error) {
  name := path.Base(root)

  if path.Base(root) != KEY_DOMAIN_ID_COUNTER {

    node, err := watched_structs.LoadThriftWatchedNode(client, root, iface.NewDomainMetadata)
    if err != nil {
      return nil, err
    }

    return &ZkDomain{name: name, metadata: node}, nil
  } else {
  return nil, nil
  }
}

// public methods

func (p *ZkDomain) GetName() string {
  return p.name
}

func (p *ZkDomain) GetId() iface.DomainID {
  return iface.DomainID(iface.AsDomainMetadata(p.metadata.Get()).ID)
}

func (p *ZkDomain) GetNumParts() int32{
  return iface.AsDomainMetadata(p.metadata.Get()).NumPartitions
}