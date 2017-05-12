package coordinator

import (
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/curator-go/curator"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"path"
  "github.com/bpodgursky/hank-go-client/iface"
)

type ZkDomain struct {
	name string

	metadata *watched_structs.ZkWatchedNode
}

func createZkDomain(ctx *serializers.ThreadCtx, root string, name string, client curator.CuratorFramework) (*ZkDomain, error) {

	metadata := hank.NewDomainMetadata()
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

func loadZkDomain(ctx *serializers.ThreadCtx, client curator.CuratorFramework, root string) (interface{}, error) {
	name := path.Base(root)

	node, err := watched_structs.LoadThriftWatchedNode(client, root, iface.NewDomainMetadata)
	if err != nil {
		return nil, err
	}

	return &ZkDomain{name: name, metadata: node}, nil

}

// public methods

func (p *ZkDomain) GetName() string {
	return p.name
}

func (p *ZkDomain) GetId(ctx *serializers.ThreadCtx) int32 {
  return iface.AsDomainMetadata(p.metadata.Get()).ID
}