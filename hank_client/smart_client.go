package hank_client

import (
	"os"
	"time"
	"fmt"
	"errors"
	"strconv"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/hank_types"
)

type HankSmartClient struct {
	coordinator iface.Coordinator
	ringGroup   iface.RingGroup

	options *hankSmartClientOptions

	hostsByAddress             map[string]*iface.PartitionServerAddress
	serverAddressToConnections map[string]*HostConnectionPool
	domainToPartToConnections  map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool
}

func NewHankSmartClient(
	coordinator iface.Coordinator,
	ringGroupName string,
	options *hankSmartClientOptions) (*HankSmartClient, error) {

	ringGroup := coordinator.GetRingGroup(ringGroupName)

	metadata, err := GetClientMetadata()

	if err != nil {
		return nil, err
	}

	ctx := serializers.NewThreadCtx()
	registerErr := ringGroup.RegisterClient(ctx, metadata)

	if registerErr != nil {
		return nil, registerErr
	}

	return &HankSmartClient{coordinator,
													ringGroup,
													options,
													make(map[string]*iface.PartitionServerAddress),
													make(map[string]*HostConnectionPool),
													make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool),
	}, nil
}

func GetClientMetadata() (*hank.ClientMetadata, error) {

	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	metadata := hank.NewClientMetadata()
	metadata.Host = hostname
	metadata.ConnectedAt = time.Now().Unix() * int64(1000)
	metadata.Type = "GolangHankSmartClient"
	metadata.Version = "lolidk"

	return metadata, nil
}

func (p *HankSmartClient) updateConnectionCache(ctx *serializers.ThreadCtx) {
	fmt.Println("Loading Hank's smart client metadata cache and connections.")

	newServerToConnections := make(map[string]*HostConnectionPool)

	newDomainToPartitionToConnections := make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool)

	p.buildNewConnectionCache(ctx, &newServerToConnections, &newDomainToPartitionToConnections)

}

func (p*HankSmartClient) isPreferredHost(host iface.Host) bool {

	fmt.Println("Environment flags for host ", host)

	//  TODO
	return false
}

func (p*HankSmartClient) buildNewConnectionCache(
	ctx *serializers.ThreadCtx,
	newServerToConnections *map[string]*HostConnectionPool,
	newDomainToPartitionToConnections *map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool) error {

	//  this is horrible looking, and I'd love to use MultiMap, but I can't  because this horseshit,
	//  gimp, special-ed language thinks that generics are too dangerous and just gives you fucking crayons
	domainToPartToAddresses := make(map[iface.DomainID]map[iface.PartitionID][]*iface.PartitionServerAddress)

	preferredHosts := []string{}

	for _, ring := range p.ringGroup.GetRings() {
		fmt.Println("Building connection cache for ", ring)

		for _, host := range ring.GetHosts(ctx) {
			fmt.Println("Building cache for host: ", host)

			if p.isPreferredHost(host) {
				preferredHosts = append(preferredHosts, host.GetAddress(ctx).Print())
			}

			address := host.GetAddress(ctx)
			fmt.Println("Loading partition metadata for Host: ", address)

			for _, hostDomain := range host.GetAssignedDomains(ctx) {

				domain, err := hostDomain.GetDomain(ctx, p.coordinator)
				if err != nil {
					return err
				}

				domainId := domain.GetId(ctx)

				if domain == nil {
					return errors.New("Domain not found " + strconv.Itoa(int(domainId)))
				}

				partitionToAddresses := domainToPartToAddresses[domainId]

				if partitionToAddresses == nil {
					domainToPartToAddresses[domainId] = make(map[iface.PartitionID][]*iface.PartitionServerAddress)
				}

				fmt.Println(address)

				for _, partition := range hostDomain.GetPartitions() {

					if !partition.IsDeletable() {

						partNum := partition.GetPartitionNumber()
						hostAddresses := partitionToAddresses[partNum]
						if hostAddresses == nil {
							hostAddresses = []*iface.PartitionServerAddress{}
						}

						partitionToAddresses[partNum] = append(hostAddresses, address)

					}
				}
			}

			addressStr := address.Print()
			pool := p.serverAddressToConnections[addressStr]
			opts := p.options

			if pool == nil {

				hostConnections := []*HostConnection{}

				fmt.Println("Establishing " + strconv.Itoa(int(opts.NumConnectionsPerHost)) + " connections to " + host.GetAddress(ctx).Print() +
					"with connection try lock timeout = " + strconv.Itoa(int(opts.TryLockTimeoutMs)) + "ms, " +
					"connection establisment timeout = " + strconv.Itoa(int(opts.EstablishConnectionTimeoutMs)) + "ms, " +
					"query timeout = " + strconv.Itoa(int(opts.QueryTimeoutMs)) + "ms")

				for i := 1; i <= int(opts.NumConnectionsPerHost); i++ {

					connection, err := NewHostConnection(
						host,
						opts.TryLockTimeoutMs,
						opts.EstablishConnectionTimeoutMs,
						opts.QueryTimeoutMs,
						opts.BulkQueryTimeoutMs,
					)

					//	TODO not totally sure what we should do on errors here.  check original impl.
					if err != nil {
						fmt.Println("Error creating host connection", err)
					}else {
						host.AddStateChangeListener(connection)
						hostConnections = append(hostConnections, connection)
					}

				}

				pool = CreateHostConnectionPool(ctx, hostConnections, -1, preferredHosts)

			}

			p.serverAddressToConnections[addressStr] = pool

		}

	}

	//TODO
	//for (Map.Entry<Integer, Map<Integer, List<HostAddress>>> domainToPartitionToAddressesEntry :
	//newDomainToPartitionToPartitionServerAddressList.entrySet()) {
	//Integer domainId = domainToPartitionToAddressesEntry.getKey();
	//Map<Integer, HostConnectionPool> partitionToConnectionPool = new HashMap<Integer, HostConnectionPool>();
	//for (Map.Entry<Integer, List<HostAddress>> partitionToAddressesEntry :
	//domainToPartitionToAddressesEntry.getValue().entrySet()) {
	//List<HostConnection> connections = new ArrayList<HostConnection>();
	//for (HostAddress address : partitionToAddressesEntry.getValue()) {
	//connections.addAll(newPartitionServerAddressToConnectionPool.get(address).getConnections());
	//}
	//Integer partitionId = partitionToAddressesEntry.getKey();
	//partitionToConnectionPool.put(partitionId,
	//HostConnectionPool.createFromList(connections, getHostListShuffleSeed(domainId, partitionId), preferredHosts));
	//}
	//newDomainToPartitionToConnectionPool.put(domainId, partitionToConnectionPool);
	//}


	return nil
}
