package iface

import (
	"strconv"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/hank_types"
)

type Coordinator interface {
	GetRingGroup(ringGroupName string) RingGroup

	AddDomainGroup(ctx *serializers.ThreadCtx, name string) (DomainGroup, error)

	GetDomainGroup(domainGroupName string) DomainGroup

	GetRingGroups() []RingGroup

	GetDomainById(ctx *serializers.ThreadCtx, domainId DomainID) (Domain, error)

	AddDomain(ctx *serializers.ThreadCtx,
		domainName string,
		numParts int32,
		storageEngineFactoryName string,
		storageEngineOptions string,
		partitionerName string,
		requiredHostFlags []string,
	) (Domain, error)

	GetDomain(domain string) Domain

	//  etc (stub for now)
}

type DomainGroup interface {
	GetName() string

	SetDomainVersions(ctx *serializers.ThreadCtx, flags map[DomainID]VersionID) error

	GetDomainVersions(ctx *serializers.ThreadCtx) []*DomainAndVersion

	GetDomainVersion(domainID DomainID) *DomainAndVersion

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

type HostState string

const (
	HOST_IDLE     HostState = "IDLE"
	HOST_SERVING  HostState = "SERVING"
	HOST_UPDATING HostState = "UPDATING"
	HOST_OFFLINE  HostState = "OFFLINE"
)

type Host interface {
	GetMetadata(ctx *serializers.ThreadCtx) *hank.HostMetadata

	GetAssignedDomains(ctx *serializers.ThreadCtx) []HostDomain

	GetEnvironmentFlags(ctx *serializers.ThreadCtx) map[string]string

	SetEnvironmentFlags(ctx *serializers.ThreadCtx, flags map[string]string) error

	AddDomain(ctx *serializers.ThreadCtx, domain Domain) (HostDomain, error)

	GetAddress() *PartitionServerAddress

	GetHostDomain(ctx *serializers.ThreadCtx, domainId DomainID) HostDomain

	AddStateChangeListener(listener serializers.DataListener)

	SetState(ctx *serializers.ThreadCtx, state HostState) error

	GetState() HostState

	//  stub
}

type Domain interface {
	//  stub

	GetName() string
	GetId() DomainID
}

type HostDomainPartition interface {
	GetPartitionNumber() PartitionID

	GetCurrentDomainVersion() VersionID

	SetCurrentDomainVersion(ctx *serializers.ThreadCtx, version VersionID) error

	IsDeletable() bool
}

type HostDomain interface {
	GetDomain(ctx *serializers.ThreadCtx, coordinator Coordinator) (Domain, error)

	AddPartition(ctx *serializers.ThreadCtx, partNum PartitionID) HostDomainPartition

	GetPartitions() []HostDomainPartition
}

type PartitionServerAddress struct {
	HostName   string
	PortNumber int32
}

func (p *PartitionServerAddress) Print() string {
	return p.HostName + ":" + strconv.Itoa(int(p.PortNumber))
}

//type HostAddress struct {
//	Ring    Ring
//	Address *PartitionServerAddress
//}

type DomainAndVersion struct {
	DomainID  DomainID
	VersionID VersionID
}
