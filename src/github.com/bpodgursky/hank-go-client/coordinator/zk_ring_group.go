package coordinator

import (
	"github.com/curator-go/curator"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"path"
	"strconv"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/bpodgursky/hank-go-client/iface"
)

const CLIENT_ROOT string = "c"
const CLIENT_NODE string = "c"

type ZkRingGroup struct {
	ringGroupPath string
	name          string
	client        curator.CuratorFramework

	clients *watched_structs.ZkWatchedMap
	rings   *watched_structs.ZkWatchedMap
}

func createZkRingGroup(ctx *serializers.ThreadCtx, client curator.CuratorFramework, name string, rootPath string) (iface.RingGroup, error) {
	rgRootPath := path.Join(rootPath, name)

	err := watched_structs.AssertEmpty(client, rgRootPath)
	if err != nil {
		return nil, err
	}

	watched_structs.CreateWithParents(client, curator.PERSISTENT, rgRootPath, nil)

	clients, err := watched_structs.NewZkWatchedMap(client, path.Join(rgRootPath, CLIENT_ROOT), loadClientMetadata)
	if err != nil {
		return nil, err
	}

	rings, err := watched_structs.NewZkWatchedMap(client, rgRootPath, loadZkRing)
	if err != nil {
		return nil, err
	}

	return &ZkRingGroup{ringGroupPath: rootPath, name: name, client: client, clients: clients, rings: rings}, nil

}

func loadZkRingGroup(ctx *serializers.ThreadCtx, rgRootPath string, client curator.CuratorFramework) (interface{}, error) {

	err := watched_structs.AssertExists(client, rgRootPath)
	if err != nil {
		return nil, err
	}

	clients, err := watched_structs.NewZkWatchedMap(client, path.Join(rgRootPath, CLIENT_ROOT), loadClientMetadata)
	if err != nil {
		return nil, err
	}

	rings, err := watched_structs.NewZkWatchedMap(client, rgRootPath, loadZkRing)

	return &ZkRingGroup{ringGroupPath: rgRootPath, client: client, clients: clients, rings: rings}, nil
}

//  loader

func loadClientMetadata(ctx *serializers.ThreadCtx, path string, client curator.CuratorFramework) (interface{}, error) {
	metadata := hank.NewClientMetadata()
	watched_structs.LoadThrift(ctx, path, client, metadata)
	return metadata, nil
}

//  methods

func (p *ZkRingGroup) RegisterClient(ctx *serializers.ThreadCtx, metadata *hank.ClientMetadata) error {
	return ctx.SetThrift(watched_structs.CreateEphemeralSequential(path.Join(p.clients.Root, CLIENT_NODE), p.client), metadata)
}

func (p *ZkRingGroup) GetName() string {
	return p.name
}

func (p *ZkRingGroup) GetClients() []*hank.ClientMetadata {

	groups := []*hank.ClientMetadata{}
	for _, item := range p.clients.Values() {
		i := item.(*hank.ClientMetadata)
		groups = append(groups, i)
	}

	return groups
}

func ringName(ringNum int) string {
	return "ring-" + strconv.Itoa(ringNum)
}

func (p *ZkRingGroup) AddRing(ctx *serializers.ThreadCtx, ringNum int) (iface.Ring, error) {
	ringChild := ringName(ringNum)
	ringRoot := path.Join(p.rings.Root, ringChild)

	ring, err := createZkRing(ctx, ringRoot, ringNum, p.client)
	if err != nil {
		return nil, err
	}

	p.rings.Put(ringChild, ring)
	return ring, nil
}

func (p *ZkRingGroup) GetRing(ringNum int) iface.Ring {
	return GetRing(ringName(ringNum), p.rings.Get)
}

func (p *ZkRingGroup) GetRings() []iface.Ring {

	rings := []iface.Ring{}
	for _, item := range p.rings.Values() {
		i := item.(iface.Ring)
		rings = append(rings, i)
	}

	return rings
}
