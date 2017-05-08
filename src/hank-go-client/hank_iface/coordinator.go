package hank_iface

import(

)
import "hank-go-client/hank_thrift"

type Coordinator interface {

  GetRingGroup(ringGroupName string) RingGroup

  AddDomainGroup(ctx *hank_thrift.ThreadCtx, name string) (DomainGroup, error)

  GetDomainGroup(domainGroupName string) DomainGroup

  GetRingGroups() []RingGroup

  //  etc (stub for now)
}
