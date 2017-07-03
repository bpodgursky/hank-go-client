package iface

import (
	"strconv"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/thriftext"
)


type DataListener interface {
	OnDataChange(newVal interface{}) error
}

type DataChangeNotifier interface {
	OnChange()
}

type NoOp struct{}

func (t *NoOp) OnDataChange(newVal interface{}) error { return nil }
func (t *NoOp) OnChange()                             {}

type Adapter struct {
	Notifier DataChangeNotifier
}

func (t *Adapter) OnDataChange(newVal interface{}) error {
	t.Notifier.OnChange()
	return nil
}

type MultiNotifier struct {
	clientListeners []DataChangeNotifier
}

func NewMultiNotifier() *MultiNotifier {
	return &MultiNotifier{clientListeners: []DataChangeNotifier{}}
}

func (p *MultiNotifier) AddClient(notifier DataChangeNotifier) {
	p.clientListeners = append(p.clientListeners, notifier)
}

func (p *MultiNotifier) OnChange() {
	for _, listener := range p.clientListeners {
		listener.OnChange()
	}
}


type Coordinator interface {
	GetRingGroup(ringGroupName string) RingGroup

	AddDomainGroup(ctx *thriftext.ThreadCtx, name string) (DomainGroup, error)

	GetDomainGroup(domainGroupName string) DomainGroup

	GetRingGroups() []RingGroup

	GetDomainById(ctx *thriftext.ThreadCtx, domainId DomainID) (Domain, error)

	AddDomain(ctx *thriftext.ThreadCtx,
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

	SetDomainVersions(ctx *thriftext.ThreadCtx, flags map[DomainID]VersionID) error

	GetDomainVersions(ctx *thriftext.ThreadCtx) []*DomainAndVersion

	GetDomainVersion(domainID DomainID) *DomainAndVersion

	//  etc (stub)
}
type Ring interface {
	//  stub

	AddHost(ctx *thriftext.ThreadCtx, hostName string, port int, hostFlags []string) (Host, error)

	GetHosts(ctx *thriftext.ThreadCtx) []Host

}

type RingGroup interface {
	GetName() string

	GetRings() []Ring

	AddRing(ctx *thriftext.ThreadCtx, ringNum RingID) (Ring, error)

	GetRing(ringNum RingID) Ring

	RegisterClient(ctx *thriftext.ThreadCtx, metadata *hank.ClientMetadata) error

	GetClients() []*hank.ClientMetadata

	AddListener(listener DataChangeNotifier)

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
	GetMetadata(ctx *thriftext.ThreadCtx) *hank.HostMetadata

	GetAssignedDomains(ctx *thriftext.ThreadCtx) []HostDomain

	GetEnvironmentFlags(ctx *thriftext.ThreadCtx) map[string]string

	SetEnvironmentFlags(ctx *thriftext.ThreadCtx, flags map[string]string) error

	AddDomain(ctx *thriftext.ThreadCtx, domain Domain) (HostDomain, error)

	GetAddress() *PartitionServerAddress

	GetHostDomain(ctx *thriftext.ThreadCtx, domainId DomainID) HostDomain

	AddStateChangeListener(listener DataListener)

	SetState(ctx *thriftext.ThreadCtx, state HostState) error

	GetState() HostState

	GetID() string

	//  stub
}

type Partitioner interface {
	Partition(key []byte, numPartitions int32) int64
}

type Domain interface {
	//  stub

	GetName() string
	GetId() DomainID

	GetPartitioner() Partitioner

	GetNumParts() int32

}

type HostDomainPartition interface {
	GetPartitionNumber() PartitionID

	GetCurrentDomainVersion() VersionID

	SetCurrentDomainVersion(ctx *thriftext.ThreadCtx, version VersionID) error

	IsDeletable() bool
}

type HostDomain interface {
	GetDomain(ctx *thriftext.ThreadCtx, coordinator Coordinator) (Domain, error)

	AddPartition(ctx *thriftext.ThreadCtx, partNum PartitionID) HostDomainPartition

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
