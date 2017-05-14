package coordinator

import (
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
)

type ZkHostDomain struct {
	host     *ZkHost
	domainId iface.DomainID
}

func newZkHostDomain(host *ZkHost, domainId iface.DomainID) *ZkHostDomain {
	return &ZkHostDomain{host: host, domainId: domainId}
}

func (p *ZkHostDomain) GetDomain(ctx *serializers.ThreadCtx, coordinator iface.Coordinator) (iface.Domain, error) {
	return coordinator.GetDomainById(ctx, p.domainId)
}

func (p *ZkHostDomain) AddPartition(ctx *serializers.ThreadCtx, partNum iface.PartitionID) iface.HostDomainPartition {
	return p.host.addPartition(ctx, p.domainId, partNum)
}


//func (p *ZkHostDomain) GetPartitions() []hank.HostDomainPartition {
//  p.host.getPar
//}
