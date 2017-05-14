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

	GetDomainById(ctx *serializers.ThreadCtx, domainId DomainID) (Domain, error)

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

  SetDomainVersions(ctx *serializers.ThreadCtx, flags map[DomainID]VersionID) error

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

	AddRing(ctx *serializers.ThreadCtx, ringNum RingID) (Ring, error)

	GetRing(ringNum RingID) Ring

	RegisterClient(ctx *serializers.ThreadCtx, metadata *hank.ClientMetadata) error

	GetClients() []*hank.ClientMetadata

	//	stub
}

type Host interface {
	GetMetadata(ctx *serializers.ThreadCtx) *hank.HostMetadata

  GetAssignedDomains(ctx *serializers.ThreadCtx) []HostDomain

  GetEnvironmentFlags(ctx *serializers.ThreadCtx) map[string]string

  SetEnvironmentFlags(ctx *serializers.ThreadCtx, flags map[string]string) error

  AddDomain(ctx *serializers.ThreadCtx, domain Domain) HostDomain

  GetAddress(ctx *serializers.ThreadCtx) *PartitionServerAddress

//  stub
}

type Domain interface {
	//  stub

	GetName() string
	GetId(ctx *serializers.ThreadCtx) DomainID
}

type HostDomainPartition interface {

  GetPartitionNumber() PartitionID

  GetCurrentDomainVersion() VersionID

}

type HostDomain interface {
	GetDomain(ctx *serializers.ThreadCtx, coordinator Coordinator) (Domain, error)

  AddPartition(ctx *serializers.ThreadCtx, partNum PartitionID) HostDomainPartition

	//GetPartitions() []HostDomainPartition
}

type PartitionServerAddress struct {
	HostName   string
	PortNumber int32
}

type HostAddress struct {
  Ring Ring
  Address *PartitionServerAddress
}