package coordinator

type Coordinator interface {

	getRingGroup(ringGroupName string) RingGroup

  addDomainGroup(domainGroupName string) DomainGroup

}
