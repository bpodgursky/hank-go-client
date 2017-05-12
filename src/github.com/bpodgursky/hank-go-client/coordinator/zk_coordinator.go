package coordinator

import (
  "github.com/curator-go/curator"
  "github.com/bpodgursky/hank-go-client/serializers"
  "github.com/bpodgursky/hank-go-client/watched_structs"
  "github.com/bpodgursky/hank-go-client/iface"
  "path"
)

type ZkCoordinator struct {
  ringGroups   *watched_structs.ZkWatchedMap
  domainGroups *watched_structs.ZkWatchedMap
  domains      *watched_structs.ZkWatchedMap
  client       curator.CuratorFramework
}

func NewZkCoordinator(client curator.CuratorFramework,
  domainsRoot string,
  ringGroupsRoot string,
  domainGroupsRoot string, ) (*ZkCoordinator, error) {

  ringGroups, rgError := watched_structs.NewZkWatchedMap(client, ringGroupsRoot, loadZkRingGroup)
  domainGroups, dgError := watched_structs.NewZkWatchedMap(client, domainGroupsRoot, loadZkDomainGroup)
  domains, dmError := watched_structs.NewZkWatchedMap(client, domainsRoot, loadZkDomain)

  if rgError != nil {
    return nil, rgError
  }

  if dgError != nil {
    return nil, dgError
  }

  if dmError != nil {
    return nil, dmError
  }

  return &ZkCoordinator{
    ringGroups,
    domainGroups,
    domains,
    client,
  }, nil

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
  numParts int,
  storageEngineFactoryName string,
  storageEngineOptions string,
  partitionerName string,
  requiredHostFlags []string) (iface.Domain, error) {

  domain, err := createZkDomain(ctx, path.Join(p.domains.Root, domainName), domainName, p.client)
  if err != nil {
    return nil, err
  }
  p.domains.Put(domainName, domain)

  return domain, nil
}

func (p *ZkCoordinator) GetDomainById(ctx *serializers.ThreadCtx, domainId int32) (iface.Domain, error) {

  for _, inst := range p.domains.Values() {
    domain := inst.(iface.Domain)
    if domain.GetId(ctx) == domainId {
      return domain, nil
    }
  }

  return nil, nil
}
