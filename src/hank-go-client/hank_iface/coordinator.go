package hank_iface

type Coordinator interface {
  GetRingGroup(ringGroupName string) RingGroup

  AddDomainGroup(domainGroupName string) DomainGroup

  GetDomainGroup(domainGroupName string) DomainGroup

  //  etc (stub for now)
}
