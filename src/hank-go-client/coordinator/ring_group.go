package coordinator

type RingGroup interface {

	getName() string

	getRings() []Ring

	addRing(ringNum int) Ring

}
