package coordinator

import (
	"github.com/curator-go/curator"
	"path"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
)

const KEY_DOMAIN_ID_COUNTER string = ".domain_id_counter"

type ZkCoordinator struct {
	ringGroups   *watched_structs.ZkWatchedMap
	domainGroups *watched_structs.ZkWatchedMap
	domains      *watched_structs.ZkWatchedMap
	client       curator.CuratorFramework

	domainIDCounter *watched_structs.ZkWatchedNode
}

func NewZkCoordinator(client curator.CuratorFramework,
	domainsRoot string,
	ringGroupsRoot string,
	domainGroupsRoot string) (*ZkCoordinator, error) {

	ringGroups, rgError := watched_structs.NewZkWatchedMap(client, ringGroupsRoot, serializers.NewMultiNotifier(), loadZkRingGroup)
	domainGroups, dgError := watched_structs.NewZkWatchedMap(client, domainGroupsRoot, serializers.NewMultiNotifier(), loadZkDomainGroup)
	domains, dmError := watched_structs.NewZkWatchedMap(client, domainsRoot, serializers.NewMultiNotifier(), loadZkDomain)

	if rgError != nil {
		return nil, rgError
	}

	if dgError != nil {
		return nil, dgError
	}

	if dmError != nil {
		return nil, dmError
	}

	counter, error := getDomainIDCounter(client, path.Join(domainsRoot, KEY_DOMAIN_ID_COUNTER))
	if error != nil {
		return nil, error
	}

	return &ZkCoordinator{
		ringGroups,
		domainGroups,
		domains,
		client,
		counter,
	}, nil

}

func getDomainIDCounter(client curator.CuratorFramework, path string) (*watched_structs.ZkWatchedNode, error) {
	domainCount, err := client.CheckExists().ForPath(path)
	if err != nil {
		return nil, err
	}

	if domainCount != nil {
		return watched_structs.LoadIntWatchedNode(client, path)
	} else {
		return watched_structs.NewIntWatchedNode(client, curator.PERSISTENT, path, -1)
	}
}

func (p *ZkCoordinator) GetRingGroup(name string) iface.RingGroup {
	return iface.AsRingGroup(p.ringGroups.Get(name))
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
	return iface.AsDomainGroup(p.domainGroups.Get(name))
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

func (p *ZkCoordinator) AddDomain(ctx *serializers.ThreadCtx,
	domainName string,
	numParts int32,
	storageEngineFactoryName string,
	storageEngineOptions string,
	partitionerName string,
	requiredHostFlags []string) (iface.Domain, error) {

	id, err := p.getNextDomainID(ctx)
	if err != nil {
		return nil, err
	}

	domain, err := createZkDomain(ctx, path.Join(p.domains.Root, domainName), domainName, iface.DomainID(id), numParts,
		storageEngineFactoryName,
		storageEngineOptions,
		partitionerName,
		requiredHostFlags,
		p.client)

	if err != nil {
		return nil, err
	}
	p.domains.Put(domainName, domain)

	return domain, nil
}

func (p *ZkCoordinator) getNextDomainID(ctx *serializers.ThreadCtx) (iface.DomainID, error) {

	val, error := p.domainIDCounter.Update(ctx, func(val interface{}) interface{} {
		nextID := val.(int)
		return nextID + 1
	})

	if error != nil {
		return -1, error
	}

	return iface.DomainID(val.(int)), nil

}

func (p *ZkCoordinator) GetDomainById(ctx *serializers.ThreadCtx, domainId iface.DomainID) (iface.Domain, error) {

	for _, inst := range p.domains.Values() {
		domain := inst.(iface.Domain)
		if domain.GetId() == domainId {
			return domain, nil
		}
	}

	return nil, nil
}

func (p *ZkCoordinator) GetDomain(domain string) (iface.Domain) {
	return iface.AsDomain(p.domains.Get(domain))
}