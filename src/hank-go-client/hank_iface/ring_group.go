package hank_iface

type RingGroup interface {

	getName() string

	getRings() []Ring

	addRing(ringNum int) Ring

	//	stub

}
