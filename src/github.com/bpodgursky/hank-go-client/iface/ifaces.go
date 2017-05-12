package iface

import (
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

type Coordinator interface {
	GetRingGroup(ringGroupName string) RingGroup

	AddDomainGroup(ctx *serializers.ThreadCtx, name string) (DomainGroup, error)

	GetDomainGroup(domainGroupName string) DomainGroup

	GetRingGroups() []RingGroup

	GetDomainById(ctx *serializers.ThreadCtx, domainId int32) (Domain, error)

	AddDomain(ctx *serializers.ThreadCtx,
		domainName string,
		numParts int,
		storageEngineFactoryName string,
		storageEngineOptions string,
		partitionerName string,
		requiredHostFlags []string,
	) (Domain, error)

	//  etc (stub for now)
}

type DomainGroup interface {
	GetName() string

	//  etc (stub)
}

type Ring interface {
	//  stub

	AddHost(ctx *serializers.ThreadCtx, hostName string, port int, hostFlags []string) (Host, error)

	GetHosts(ctx *serializers.ThreadCtx) []Host
}

type RingGroup interface {
	GetName() string

	GetRings() []Ring

	AddRing(ctx *serializers.ThreadCtx, ringNum int) (Ring, error)

	GetRing(ringNum int) Ring

	RegisterClient(ctx *serializers.ThreadCtx, metadata *hank.ClientMetadata) error

	GetClients() []*hank.ClientMetadata

	//	stub
}

type Host interface {
	GetMetadata(ctx *serializers.ThreadCtx) *hank.HostMetadata

	GetAssignedDomains(ctx *serializers.ThreadCtx) ([]HostDomain, error)

	//  stub
}

type Domain interface {
	//  stub

	GetName() string
	GetId(ctx *serializers.ThreadCtx) int32
}

type HostDomainPartition interface {
}

type HostDomain interface {
	GetDomain(ctx *serializers.ThreadCtx) Domain

	GetPartitions() []HostDomainPartition
}

type PartitionServerAddress struct {
	HostName   string
	PortNumber int
}
