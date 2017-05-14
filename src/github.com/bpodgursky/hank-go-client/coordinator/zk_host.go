package coordinator

import (
  "github.com/curator-go/curator"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "strings"
  "github.com/bpodgursky/hank-go-client/serializers"
  "github.com/bpodgursky/hank-go-client/watched_structs"
  "github.com/bpodgursky/hank-go-client/iface"
  "path"
)

const ASSIGNMENTS_PATH string = "a"

type ZkHost struct {
  path string

  metadata           *watched_structs.ZkWatchedNode
  assignedPartitions *watched_structs.ZkWatchedNode
}

func createZkHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, rootPath string, hostName string, port int, flags []string) (iface.Host, error) {

  metadata := hank.NewHostMetadata()
  metadata.HostName = hostName
  metadata.PortNumber = int32(port)
  metadata.Flags = strings.Join(flags, ",")

  node, err := watched_structs.NewThriftWatchedNode(client, curator.PERSISTENT, rootPath, ctx, iface.NewHostMetadata, metadata)
  if err != nil {
    return nil, err
  }

  assignmentMetadata := hank.NewHostAssignmentsMetadata()
  assignmentMetadata.Domains = make(map[int32]*hank.HostDomainMetadata)

  partitionAssignments, err := watched_structs.NewThriftWatchedNode(client,
    curator.PERSISTENT,
    assignmentsRoot(rootPath),
    ctx,
    iface.NewHostAssignmentMetadata,
    assignmentMetadata)
  if err != nil {
    return nil, err
  }

  return &ZkHost{rootPath, node, partitionAssignments}, nil
}
func assignmentsRoot(rootPath string) string {
  return path.Join(rootPath, ASSIGNMENTS_PATH)
}

func loadZkHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, rootPath string) (interface{}, error) {

  node, err := watched_structs.LoadThriftWatchedNode(client, rootPath, iface.NewHostMetadata)
  if err != nil {
    return nil, err
  }

  assignments, err := watched_structs.LoadThriftWatchedNode(client, assignmentsRoot(rootPath), iface.NewHostAssignmentMetadata)
  if err != nil {
    return nil, err
  }

  return &ZkHost{rootPath, node, assignments}, nil
}

func (p *ZkHost) addPartition(ctx *serializers.ThreadCtx, domainId iface.DomainID, partNum iface.PartitionID) iface.HostDomainPartition {

  p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
    metadata := iface.AsHostAssignmentsMetadata(orig)

    if _, ok := metadata.Domains[int32(domainId)]; !ok {
      metadata.Domains[int32(domainId)] = hank.NewHostDomainMetadata()
    }

    partitionMetadata := hank.NewHostDomainPartitionMetadata()
    partitionMetadata.Deletable = false

    metadata.Domains[int32(domainId)].Partitions[int32(partNum)] = partitionMetadata
    return metadata
  })

  return newZkHostDomainPartition(p, domainId, partNum)
}

//  for zk impls

func (p *ZkHost) getCurrentDomainGroupVersion(domainId iface.DomainID, partitionNumber iface.PartitionID) iface.VersionID {

  domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]

  if domainMetadata == nil {
    return iface.NO_VERSION
  }

  partitionMetadata := domainMetadata.Partitions[int32(partitionNumber)]

  if partitionMetadata == nil {
    return iface.NO_VERSION
  }

  if partitionMetadata.CurrentVersionNumber == nil {
    return iface.NO_VERSION
  }

  return iface.VersionID(*partitionMetadata.CurrentVersionNumber)
}

//  public methods

func (p *ZkHost) GetMetadata(ctx *serializers.ThreadCtx) *hank.HostMetadata {
  return iface.AsHostMetadata(p.metadata.Get())
}

func (p *ZkHost) GetAssignedDomains(ctx *serializers.ThreadCtx) []iface.HostDomain {

  assignedDomains := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get())

  hostDomains := []iface.HostDomain{}
  for domainId := range assignedDomains.Domains {
    hostDomains = append(hostDomains, newZkHostDomain(p, iface.DomainID(domainId)))
  }

  return hostDomains
}

func (p *ZkHost) GetEnvironmentFlags(ctx *serializers.ThreadCtx) map[string]string {
  return iface.AsHostMetadata(p.metadata.Get()).EnvironmentFlags
}

func (p *ZkHost) SetEnvironmentFlags(ctx *serializers.ThreadCtx, flags map[string]string) error {

  _, err := p.metadata.Update(ctx, func(val interface{}) interface{} {
    metadata := iface.AsHostMetadata(val)
    metadata.EnvironmentFlags = flags
    return metadata
  })

  return err
}

func (p *ZkHost) AddDomain(ctx *serializers.ThreadCtx, domain iface.Domain) iface.HostDomain {
  domainId := domain.GetId(ctx)

  p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
    metadata := iface.AsHostAssignmentsMetadata(orig)
    if _, ok := metadata.Domains[int32(domainId)]; !ok {
      metadata.Domains[int32(domainId)] = hank.NewHostDomainMetadata()
    }
    return metadata
  })

  return newZkHostDomain(p, domainId)
}

func (p *ZkHost) GetAddress(ctx *serializers.ThreadCtx) *iface.PartitionServerAddress {
  metadata := iface.AsHostMetadata(p.metadata.Get())
  return &iface.PartitionServerAddress{HostName: metadata.HostName, PortNumber: metadata.PortNumber}
}
