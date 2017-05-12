package coordinator

import (
  "github.com/bpodgursky/hank-go-client/serializers"
  "github.com/bpodgursky/hank-go-client/iface"
)

type ZkHostDomain struct {
  host     *ZkHost
  domainId int32
}

func newZkHostDomain(host *ZkHost, domainId int32) *ZkHostDomain {
  return &ZkHostDomain{host: host, domainId: domainId}
}

func (p *ZkHostDomain) GetDomain(ctx *serializers.ThreadCtx, coordinator iface.Coordinator) (iface.Domain, error) {
  return coordinator.GetDomainById(ctx, p.domainId)
}

//func (p *ZkHostDomain) AddPartition(partNum int32) iface.HostDomainPartition {
//  p.host.addPartition(p.domainId, partNum)
//
//
//
//}


//
//func GetPartitions() []hank.HostDomainPartition {
//
//}
