package coordinator

import (
  "github.com/curator-go/curator"
)

type ZkCoordinator struct {
  ringGroups   *ZkWatchedMap
  domainGroups *ZkWatchedMap
  client       curator.CuratorFramework
}

func NewZkCoordinator(client curator.CuratorFramework,
  ringGroupsRoot string,
  domainGroupsRoot string) (*ZkCoordinator, error) {

  ringGroups, rgError := NewZkWatchedMap(client, ringGroupsRoot, &ZkRingGroupLoader{})
  domainGroups, dmError := NewZkWatchedMap(client, domainGroupsRoot, &ZkDomainGroupLoader{})

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

func (p *ZkCoordinator) getRingGroup(name string) RingGroup {

  ringGroup := p.ringGroups.Get(name)
  original, ok := ringGroup.(RingGroup)

  if ok {
    return original
  }

  return nil
}

func (p *ZkCoordinator) getDomainGroup(name string) DomainGroup {

  domainGroup := p.domainGroups.Get(name)

  if domainGroup == nil {
    return nil
  }

  original, ok := domainGroup.(DomainGroup)

  if ok {
    return original
  }

  return nil
}

func (p *ZkCoordinator) addDomainGroup(name string) (DomainGroup, error) {

  group, error := CreateZkDomainGroup(p.client, name, p.domainGroups.Root)
  if error != nil {
    return nil, error
  }

  p.domainGroups.Put(name, group)
  return group, nil
}
