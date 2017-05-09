package coordinator

import (
	"github.com/curator-go/curator"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"path"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/watched_structs"
)

type ZkDomainGroup struct {
	name     string
	metadata *watched_structs.ZkWatchedNode
}

func createZkDomainGroup(ctx *serializers.ThreadCtx, client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error) {

	metadataPath := path.Join(rootPath, name)

	err := watched_structs.AssertEmpty(client, metadataPath)
	if err != nil {
		return nil, err
	}

	metadata := hank.NewDomainGroupMetadata()
	metadata.DomainVersions = make(map[int32]int32)

	node, nodeErr := watched_structs.NewThriftZkWatchedNode(
		client,
		curator.PERSISTENT,
		metadataPath,
		ctx,
		metadata,
	)

	if nodeErr != nil {
		return nil, nodeErr
	}

	return &ZkDomainGroup{name: name, metadata: node}, nil
}

func loadZkDomainGroup(ctx *serializers.ThreadCtx, fullPath string, client curator.CuratorFramework) (interface{}, error) {

	name := path.Base(fullPath)

	err := watched_structs.AssertExists(client, fullPath)
	if err != nil {
		return nil, err
	}

	node, nodeErr := watched_structs.LoadZkWatchedNode(client, fullPath)
	if nodeErr != nil {
		return nil, nodeErr
	}

	return &ZkDomainGroup{name: name, metadata: node}, nil
}

//  public stuff

func (p *ZkDomainGroup) GetName() string {
	return p.name
}

func (p *ZkDomainGroup) GetDomainVersions(ctx *serializers.ThreadCtx) {
	GetDomainGroupMetadata(ctx, p.metadata.Get)
}
