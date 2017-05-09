package coordinator

import (
	"github.com/curator-go/curator"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/bpodgursky/hank-go-client/iface"
)

type ZkCoordinator struct {
	ringGroups   *watched_structs.ZkWatchedMap
	domainGroups *watched_structs.ZkWatchedMap
	client       curator.CuratorFramework
}

func NewZkCoordinator(client curator.CuratorFramework,
	ringGroupsRoot string,
	domainGroupsRoot string) (*ZkCoordinator, error) {

	ringGroups, rgError := watched_structs.NewZkWatchedMap(client, ringGroupsRoot, loadZkRingGroup)
	domainGroups, dmError := watched_structs.NewZkWatchedMap(client, domainGroupsRoot, loadZkDomainGroup)

	if rgError != nil {
		return nil, rgError
	}

	if dmError != nil {
		return nil, dmError
	}

	return &ZkCoordinator{
		ringGroups:   ringGroups,
		domainGroups: domainGroups,
		client:       client,
	}, nil

}

func (p *ZkCoordinator) GetRingGroup(name string) iface.RingGroup {
	return GetRingGroup(name, p.ringGroups.Get)
}

func (p *ZkCoordinator) GetRingGroups() []iface.RingGroup {

	groups := []iface.RingGroup{}
	for _, item := range p.ringGroups.Values() {
		i := item.(iface.RingGroup)
		groups = append(groups, i)
	}

	return groups

}

func (p *ZkCoordinator) GetDomainGroup(name string) iface.DomainGroup {
	return GetDomainGroup(name, p.domainGroups.Get)
}

func (p *ZkCoordinator) AddDomainGroup(ctx *serializers.ThreadCtx, name string) (iface.DomainGroup, error) {

	group, err := createZkDomainGroup(ctx, p.client, name, p.domainGroups.Root)
	if err != nil {
		return nil, err
	}

	p.domainGroups.Put(name, group)
	return group, nil

}

func (p *ZkCoordinator) AddRingGroup(ctx *serializers.ThreadCtx, name string) (iface.RingGroup, error) {

	group, err := createZkRingGroup(ctx, p.client, name, p.ringGroups.Root)
	if err != nil {
		return nil, err
	}
	p.ringGroups.Put(name, group)

	return group, nil
}
