package coordinator

import (
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/curator-go/curator"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"path"
	"strings"
)

const ASSIGNMENTS_PATH string = "a"
const STATE_PATH string = "s"

type ZkHost struct {
	path string

	metadata           *watched_structs.ZkWatchedNode
	assignedPartitions *watched_structs.ZkWatchedNode
	state              *watched_structs.ZkWatchedNode
}

func CreateZkHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, rootPath string, hostName string, port int, flags []string) (iface.Host, error) {

	metadata := hank.NewHostMetadata()
	metadata.HostName = hostName
	metadata.PortNumber = int32(port)
	metadata.Flags = strings.Join(flags, ",")

	node, err := watched_structs.NewThriftWatchedNode(client, curator.PERSISTENT, rootPath, ctx, iface.NewHostMetadata, metadata)
	if err != nil {
		return nil, err
	}

	assignmentMetadata := hank.NewHostAssignmentsMetadata()
	assignmentMetadata.Domains = make(map[int32]*hank.HostDomainMetadata)

	partitionAssignments, err := watched_structs.NewThriftWatchedNode(client,
		curator.PERSISTENT,
		assignmentsRoot(rootPath),
		ctx,
		iface.NewHostAssignmentMetadata,
		assignmentMetadata)
	if err != nil {
		return nil, err
	}

	state, err := watched_structs.NewIntWatchedNode(client,
		curator.EPHEMERAL,
		path.Join(rootPath, STATE_PATH),
		int(iface.HOST_OFFLINE))

	return &ZkHost{rootPath, node, partitionAssignments, state}, nil
}
func assignmentsRoot(rootPath string) string {
	return path.Join(rootPath, ASSIGNMENTS_PATH)
}

func loadZkHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, rootPath string) (interface{}, error) {

	node, err := watched_structs.LoadThriftWatchedNode(client, rootPath, iface.NewHostMetadata)
	if err != nil {
		return nil, err
	}

	assignments, err := watched_structs.LoadThriftWatchedNode(client, assignmentsRoot(rootPath), iface.NewHostAssignmentMetadata)
	if err != nil {
		return nil, err
	}

	state, err := watched_structs.LoadIntWatchedNode(client,
		path.Join(rootPath, STATE_PATH))

	return &ZkHost{rootPath, node, assignments, state}, nil
}

func (p *ZkHost) addPartition(ctx *serializers.ThreadCtx, domainId iface.DomainID, partNum iface.PartitionID) iface.HostDomainPartition {

	p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
		metadata := iface.AsHostAssignmentsMetadata(orig)

		if _, ok := metadata.Domains[int32(domainId)]; !ok {
			metadata.Domains[int32(domainId)] = hank.NewHostDomainMetadata()
		}

		partitionMetadata := hank.NewHostDomainPartitionMetadata()
		partitionMetadata.Deletable = false

		metadata.Domains[int32(domainId)].Partitions[int32(partNum)] = partitionMetadata
		return metadata
	})

	return newZkHostDomainPartition(p, domainId, partNum)
}

//  for zk impls

func (p *ZkHost) getCurrentDomainGroupVersion(domainId iface.DomainID, partitionNumber iface.PartitionID) iface.VersionID {

	domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]
	if domainMetadata == nil {
		return iface.NO_VERSION
	}

	partitionMetadata := domainMetadata.Partitions[int32(partitionNumber)]
	if partitionMetadata == nil || partitionMetadata.CurrentVersionNumber == nil {
		return iface.NO_VERSION
	}

	return iface.VersionID(*partitionMetadata.CurrentVersionNumber)
}

func (p *ZkHost) isDeletable(domainId iface.DomainID, partitionNumber iface.PartitionID) bool {

	domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]
	if domainMetadata == nil {
		return false
	}

	partitionMetadata := domainMetadata.Partitions[int32(partitionNumber)]
	if partitionMetadata == nil {
		return false
	}

	return partitionMetadata.Deletable
}

func (p *ZkHost) setCurrentDomainGroupVersion(ctx *serializers.ThreadCtx, domainId iface.DomainID, partitionNumber iface.PartitionID, version iface.VersionID) error {

	_, err := p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
		metadata := iface.AsHostAssignmentsMetadata(orig)
		ensureDomain(metadata, domainId)

		partitionMetadata := hank.NewHostDomainPartitionMetadata()
		thisVariableExistsBecauseGoIsAStupidLanguage := int32(version)
		partitionMetadata.CurrentVersionNumber = &thisVariableExistsBecauseGoIsAStupidLanguage
		partitionMetadata.Deletable = false

		metadata.Domains[int32(domainId)].Partitions[int32(partitionNumber)] = partitionMetadata

		return metadata
	})

	return err
}

func (p *ZkHost) getPartitions(domainId iface.DomainID) []iface.HostDomainPartition {
	domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]

	var values []iface.HostDomainPartition
	for key := range domainMetadata.Partitions {
		values = append(values, newZkHostDomainPartition(p, domainId, iface.PartitionID(key)))
	}

	return values
}

//  public

func (p *ZkHost) AddStateChangeListener(listener serializers.DataListener) {
	p.state.AddListener(listener)
}

func (p *ZkHost) GetMetadata(ctx *serializers.ThreadCtx) *hank.HostMetadata {
	return iface.AsHostMetadata(p.metadata.Get())
}

func (p *ZkHost) GetAssignedDomains(ctx *serializers.ThreadCtx) []iface.HostDomain {
	assignedDomains := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get())

	hostDomains := []iface.HostDomain{}
	for domainId := range assignedDomains.Domains {
		hostDomains = append(hostDomains, newZkHostDomain(p, iface.DomainID(domainId)))
	}

	return hostDomains
}

func (p *ZkHost) GetEnvironmentFlags(ctx *serializers.ThreadCtx) map[string]string {
	return iface.AsHostMetadata(p.metadata.Get()).EnvironmentFlags
}

func (p *ZkHost) SetEnvironmentFlags(ctx *serializers.ThreadCtx, flags map[string]string) error {

	_, err := p.metadata.Update(ctx, func(val interface{}) interface{} {
		metadata := iface.AsHostMetadata(val)
		metadata.EnvironmentFlags = flags
		return metadata
	})

	return err
}

func (p *ZkHost) SetState(ctx *serializers.ThreadCtx, state iface.HostState) error {
	return p.state.Set(ctx, int(state))
}

func (p *ZkHost) GetState() iface.HostState {
	return iface.HostState(p.state.Get().(int))
}

func (p *ZkHost) AddDomain(ctx *serializers.ThreadCtx, domain iface.Domain) (iface.HostDomain, error) {
	domainId := domain.GetId(ctx)

	_, err := p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
		metadata := iface.AsHostAssignmentsMetadata(orig)
		ensureDomain(metadata, domainId)
		return metadata
	})

	if err != nil {
		return nil, err
	}

	return newZkHostDomain(p, domainId), nil
}
func ensureDomain(metadata *hank.HostAssignmentsMetadata, domainId iface.DomainID) {
	if _, ok := metadata.Domains[int32(domainId)]; !ok {
		domainMetadata := hank.NewHostDomainMetadata()
		domainMetadata.Partitions = make(map[int32]*hank.HostDomainPartitionMetadata)

		metadata.Domains[int32(domainId)] = domainMetadata
	}
}

func (p *ZkHost) GetAddress(ctx *serializers.ThreadCtx) *iface.PartitionServerAddress {
	metadata := iface.AsHostMetadata(p.metadata.Get())
	return &iface.PartitionServerAddress{HostName: metadata.HostName, PortNumber: metadata.PortNumber}
}

func (p *ZkHost) GetHostDomain(ctx *serializers.ThreadCtx, domainId iface.DomainID) iface.HostDomain {

	assignedDomains := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get())
	metadata := assignedDomains.Domains[int32(domainId)]

	if metadata == nil {
		return nil
	}

	return newZkHostDomain(p, domainId)

}
