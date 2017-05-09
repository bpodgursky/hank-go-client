package coordinator

import (
	"github.com/curator-go/curator"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"strings"
  "github.com/bpodgursky/hank-go-client/serializers"
  "github.com/bpodgursky/hank-go-client/watched_structs"
  "github.com/bpodgursky/hank-go-client/iface"
)

type ZkHost struct {
	path string

	metadata *watched_structs.ZkWatchedNode
}

func createZkHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, rootPath string, hostName string, port int, flags []string) (iface.Host, error) {

	metadata := hank.NewHostMetadata()
	metadata.HostName = hostName
	metadata.PortNumber = int32(port)
	metadata.Flags = strings.Join(flags, ",")

	node, err := watched_structs.NewThriftZkWatchedNode(client, curator.PERSISTENT, rootPath, ctx, metadata)
	if err != nil {
		return nil, err
	}

	return &ZkHost{path: rootPath, metadata: node}, nil
}

func loadZkHost(ctx *serializers.ThreadCtx, rootPath string, client curator.CuratorFramework) (interface{}, error) {

	node, err := watched_structs.LoadZkWatchedNode(client, rootPath)
	if err != nil {
		return nil, err
	}

	return &ZkHost{path: rootPath, metadata: node}, nil
}

//  public methods

func (p *ZkHost) GetMetadata(ctx *serializers.ThreadCtx) (*hank.HostMetadata, error) {
	return GetHostMetadata(ctx, p.metadata.Get)
}

func (p *ZkHost) GetAssignedDomains(ctx *serializers.ThreadCtx) ([]iface.HostDomain, error) {
  //  TODO
  return nil, nil
}
