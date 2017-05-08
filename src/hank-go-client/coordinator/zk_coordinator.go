package coordinator

import (
  "github.com/curator-go/curator"
  "hank-go-client/hank_zk"
  "hank-go-client/hank_iface"
  "hank-go-client/hank_util"
  "hank-go-client/hank_thrift"
)

type ZkCoordinator struct {
  ringGroups   *hank_zk.ZkWatchedMap
  domainGroups *hank_zk.ZkWatchedMap
  client       curator.CuratorFramework
}

func NewZkCoordinator(client curator.CuratorFramework,
  ringGroupsRoot string,
  domainGroupsRoot string) (*ZkCoordinator, error) {

  ringGroups, rgError := hank_zk.NewZkWatchedMap(client, ringGroupsRoot, loadZkRingGroup)
  domainGroups, dmError := hank_zk.NewZkWatchedMap(client, domainGroupsRoot, loadZkDomainGroup)

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
  return hank_util.GetRingGroup(name, p.ringGroups.Get)
}

func (p *ZkCoordinator) GetRingGroups() []hank_iface.RingGroup {

  groups := []hank_iface.RingGroup{}
  for _,item := range p.ringGroups.Values() {
    i := item.(hank_iface.RingGroup)
    groups = append(groups, i)
  }

  return groups

}

func (p *ZkCoordinator) GetDomainGroup(name string) hank_iface.DomainGroup {
  return hank_util.GetDomainGroup(name, p.domainGroups.Get)
}

func (p *ZkCoordinator) AddDomainGroup(ctx *hank_thrift.ThreadCtx, name string) (hank_iface.DomainGroup, error) {

  group, err := createZkDomainGroup(ctx, p.client, name, p.domainGroups.Root)
  if err != nil {
    return nil, err
  }

  p.domainGroups.Put(name, group)
  return group, nil

}

func (p *ZkCoordinator) AddRingGroup(ctx *hank_thrift.ThreadCtx, name string) (hank_iface.RingGroup, error) {

  group, err := createZkRingGroup(ctx, p.client, name, p.ringGroups.Root)
  if err != nil {
    return nil, err
  }
  p.ringGroups.Put(name, group)

  return group, nil
}
