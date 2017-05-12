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

func (p *ZkHost) addPartition(domainId int32, partNum int32){




}

//  public methods

func (p *ZkHost) GetMetadata(ctx *serializers.ThreadCtx) *hank.HostMetadata {
  return iface.AsHostMetadata(p.metadata.Get())
}

func (p *ZkHost) GetAssignedDomains(ctx *serializers.ThreadCtx) ([]iface.HostDomain, error) {
  //  TODO

  //assignedPartitions

  return nil, nil
}
