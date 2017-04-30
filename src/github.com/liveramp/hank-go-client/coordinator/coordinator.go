package coordinator

type Coordinator interface {

	getRingGroup(ringGroupName string) RingGroup

}
