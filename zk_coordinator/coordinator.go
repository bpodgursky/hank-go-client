package zk_coordinator

import (
	"github.com/curator-go/curator"
	"path"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/thriftext"
	"github.com/bpodgursky/hank-go-client/curatorext"
)

const KEY_DOMAIN_ID_COUNTER string = ".domain_id_counter"

type ZkCoordinator struct {
	ringGroups   *curatorext.ZkWatchedMap
	domainGroups *curatorext.ZkWatchedMap
	domains      *curatorext.ZkWatchedMap
	client       curator.CuratorFramework

	domainIDCounter *curatorext.ZkWatchedNode
}

func NewZkCoordinator(client curator.CuratorFramework,
	domainsRoot string,
	ringGroupsRoot string,
	domainGroupsRoot string) (*ZkCoordinator, error) {

	ringGroups, rgError := curatorext.NewZkWatchedMap(client, ringGroupsRoot, iface.NewMultiNotifier(), loadZkRingGroup)
	domainGroups, dgError := curatorext.NewZkWatchedMap(client, domainGroupsRoot, iface.NewMultiNotifier(), loadZkDomainGroup)
	domains, dmError := curatorext.NewZkWatchedMap(client, domainsRoot, iface.NewMultiNotifier(), loadZkDomain)

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

func getDomainIDCounter(client curator.CuratorFramework, path string) (*curatorext.ZkWatchedNode, error) {
	domainCount, err := client.CheckExists().ForPath(path)
	if err != nil {
		return nil, err
	}

	if domainCount != nil {
		return curatorext.LoadIntWatchedNode(client, path)
	} else {
		return curatorext.NewIntWatchedNode(client, curator.PERSISTENT, path, -1)
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

func (p *ZkCoordinator) AddDomainGroup(ctx *thriftext.ThreadCtx, name string) (iface.DomainGroup, error) {

	group, err := createZkDomainGroup(ctx, p.client, name, p.domainGroups.Root)
	if err != nil {
		return nil, err
	}

	err = curatorext.WaitUntilOrErr(func() bool {
		return p.domainGroups.Contains(name)
	})
	if err != nil{
		return nil, err
	}

	return group, nil

}

func (p *ZkCoordinator) AddRingGroup(ctx *thriftext.ThreadCtx, name string) (iface.RingGroup, error) {

	group, err := createZkRingGroup(ctx, p.client, name, p.ringGroups.Root)
	if err != nil {
		return nil, err
	}

	err = curatorext.WaitUntilOrErr(func() bool {
		return p.ringGroups.Contains(name)
	})
	if err != nil{
		return nil, err
	}

	//	TODO clean up
	p.ringGroups.Put(name, group)

	return group, nil
}

func (p *ZkCoordinator) AddDomain(ctx *thriftext.ThreadCtx,
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

	err = curatorext.WaitUntilOrErr(func() bool {
		return p.domains.Contains(domainName)
	})
	if err != nil{
		return nil, err
	}

	return domain, nil
}

func (p *ZkCoordinator) getNextDomainID(ctx *thriftext.ThreadCtx) (iface.DomainID, error) {

	val, error := p.domainIDCounter.Update(ctx, func(val interface{}) interface{} {
		nextID := val.(int)
		return nextID + 1
	})

	if error != nil {
		return -1, error
	}

	return iface.DomainID(val.(int)), nil

}

func (p *ZkCoordinator) GetDomainById(ctx *thriftext.ThreadCtx, domainId iface.DomainID) (iface.Domain, error) {

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