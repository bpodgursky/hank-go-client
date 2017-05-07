package coordinator

import (
  "github.com/curator-go/curator"
  "hank-go-client/hank_zk"
  "hank-go-client/hank_iface"
  "hank-go-client/hank_util"
)

type ZkCoordinator struct {
  ringGroups   *hank_zk.ZkWatchedMap
  domainGroups *hank_zk.ZkWatchedMap
  client       curator.CuratorFramework
}

func NewZkCoordinator(client curator.CuratorFramework,
  ringGroupsRoot string,
  domainGroupsRoot string) (*ZkCoordinator, error) {

  ringGroups, rgError := hank_zk.NewZkWatchedMap(client, ringGroupsRoot, LoadZkRingGroup)
  domainGroups, dmError := hank_zk.NewZkWatchedMap(client, domainGroupsRoot, LoadZkDomainGroup)

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

func (p *ZkCoordinator) GetRingGroup(name string) hank_iface.RingGroup {
  return hank_util.GetRingGroup(name, p.domainGroups.Get)
}

func (p *ZkCoordinator) GetDomainGroup(name string) hank_iface.DomainGroup {
  return hank_util.GetDomainGroup(name, p.domainGroups.Get)
}

func (p *ZkCoordinator) AddDomainGroup(name string) (hank_iface.DomainGroup, error) {

  group, err := CreateZkDomainGroup(p.client, name, p.domainGroups.Root)
  if err != nil {
    return nil, err
  }

  p.domainGroups.Put(name, group)
  return group, nil

}
