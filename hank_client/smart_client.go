package hank_client

import (
	"errors"
	"fmt"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
	"github.com/karlseguin/ccache"
	"github.com/bpodgursky/hank-go-client/syncext"
)

type RequestCounters struct {
	requests  int64
	cacheHits int64

	lock *sync.Mutex
}

func NewRequestCounters() *RequestCounters {
	return &RequestCounters{
		0,
		0,
		&sync.Mutex{},
	}
}

func (p *RequestCounters) increment(requests int64, cacheHits int64) {
	p.lock.Lock()

	p.requests++
	p.cacheHits++

	p.lock.Unlock()

}

type HankSmartClient struct {
	coordinator iface.Coordinator
	ringGroup   iface.RingGroup

	options *hankSmartClientOptions

	hostsByAddress            map[string]*iface.PartitionServerAddress
	serverToConnections       map[string]*HostConnectionPool
	domainToPartToConnections map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool
	connectionLock            *sync.Mutex

	cache    *ccache.Cache
	counters *RequestCounters

	cacheUpdateLock *syncext.SingleLockSemaphore
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

	ctx := iface.NewThreadCtx()
	registerErr := ringGroup.RegisterClient(ctx, metadata)

	if registerErr != nil {
		return nil, registerErr
	}

	var cache *ccache.Cache

	if options.ResponseCacheEnabled {
		cache = ccache.New(ccache.Configure().MaxSize(int64(options.ResponseCacheNumItems)))
	}

	connectionCacheLock := syncext.NewSingleLockSemaphore()

	client := &HankSmartClient{coordinator,
														 ringGroup,
														 options,
														 make(map[string]*iface.PartitionServerAddress),
														 make(map[string]*HostConnectionPool),
														 make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool),
														 &sync.Mutex{},
														 cache,
														 NewRequestCounters(),
														 connectionCacheLock,
	}

	client.updateConnectionCache(ctx)

	stopping := false
	go client.updateLoop(&stopping, connectionCacheLock)

	ringGroup.AddListener(client)

	return client, nil
}

func (p *HankSmartClient) OnChange() {
	p.cacheUpdateLock.Release()
}

func (p *HankSmartClient) updateLoop(stopping *bool, listenerLock *syncext.SingleLockSemaphore) {

	ctx := iface.NewThreadCtx()

	for true {
		listenerLock.Read()

		if *stopping {
			break
		}

		p.updateConnectionCache(ctx)
	}

}

func (p *HankSmartClient) Stop() {

	for _, value := range p.domainToPartToConnections {
		for _, connections := range value {
			for _, conns := range connections.otherPools.connections {
				for _, conn := range conns {
					conn.connection.Disconnect()
				}
			}
		}
	}

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

func (p *HankSmartClient) updateConnectionCache(ctx *iface.ThreadCtx) {
	fmt.Println("Loading Hank's smart client metadata cache and connections.")

	newServerToConnections := make(map[string]*HostConnectionPool)
	newDomainToPartitionToConnections := make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool)

	p.buildNewConnectionCache(ctx, newServerToConnections, newDomainToPartitionToConnections)

	oldServerToConnections := p.serverToConnections

	// Switch old cache for new cache
	p.connectionLock.Lock()
	p.serverToConnections = newServerToConnections
	p.domainToPartToConnections = newDomainToPartitionToConnections
	p.connectionLock.Unlock()

	for address, pool := range oldServerToConnections {
		if _, ok := p.serverToConnections[address]; !ok {
			for _, conn := range pool.GetConnections() {
				conn.Disconnect()
			}
		}
	}

}

func noSuchDomain() *hank.HankResponse {
	resp := hank.NewHankResponse()
	exception := hank.NewHankException()
	exception.NoSuchDomain = newTrue()
	resp.Xception = exception
	return resp
}

func noReplica() *hank.HankResponse {
	resp := hank.NewHankResponse()
	exception := hank.NewHankException()
	exception.NoReplica = newTrue()
	resp.Xception = exception
	return resp
}

func (p *HankSmartClient) Get(domainName string, key []byte) (*hank.HankResponse, error) {

	domain := p.coordinator.GetDomain(domainName)

	if domain == nil {
		fmt.Printf("No domain found: %v\n", domainName)
		return noSuchDomain(), nil
	}

	return p.get(domain, key)
}

type Entry struct {
	domain iface.DomainID
	key    string
}

func (p *HankSmartClient) get(domain iface.Domain, key []byte) (*hank.HankResponse, error) {

	if key == nil {
		return nil, errors.New("Null key")
	}

	if len(key) == 0 {
		return nil, errors.New("Empty key")
	}

	domainID := domain.GetId()
	entry := strconv.Itoa(int(domainID)) + "-" + string(key)

	var val interface{}

	if p.cache != nil {
		item := p.cache.Get(entry)
		if item != nil && !item.Expired() {
			val = item.Value()
		}
	}

	if val != nil {
		p.counters.increment(1, 1)
		return val.(*hank.HankResponse), nil
	} else {
		p.counters.increment(1, 0)

		// Determine HostConnectionPool to use
		partitioner := domain.GetPartitioner()
		partition := partitioner.Partition(key, domain.GetNumParts())
		keyHash := partitioner.Partition(key, math.MaxInt32)

		p.connectionLock.Lock()
		parts := p.domainToPartToConnections[domainID]
		p.connectionLock.Unlock()

		if parts == nil {
			fmt.Printf("Could not find domain to partition map for domain %v (id: %v)]\n", domain.GetName(), domainID)
			return noReplica(), nil
		}

		pool := parts[iface.PartitionID(partition)]

		if pool == nil {
			fmt.Printf("Could not find list of hosts for domain %v, partition %v\n", domain.GetName(), partition)

			fmt.Println(parts)

			return noReplica(), nil
		}

		response := pool.Get(domain, key, p.options.QueryMaxNumTries, keyHash)

		if p.cache != nil && response != nil && (response.IsSetNotFound() || response.IsSetValue()) {
			p.cache.Set(entry, response, p.options.ResponseCacheExpiryTime)
		}

		if response.IsSetXception() {
			fmt.Printf("Failed to perform get: domain: %v partition; %v key; %v", domain, partition, key)
		}

		return response, nil

	}

}

func (p *HankSmartClient) isPreferredHost(ctx *iface.ThreadCtx, host iface.Host) bool {

	fmt.Println("Environment flags for host ", host)

	flags := host.GetEnvironmentFlags(ctx)

	if flags != nil && p.options.PreferredHostEnvironment != nil {
		clientValue, ok := flags[p.options.PreferredHostEnvironment.Key]

		if ok && clientValue == p.options.PreferredHostEnvironment.Value {
			return true
		}

	}

	return false
}

func (p *HankSmartClient) buildNewConnectionCache(
	ctx *iface.ThreadCtx,
	newServerToConnections map[string]*HostConnectionPool,
	newDomainToPartitionToConnections map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool) error {

	//  this is horrible looking, and I'd love to make a MultiMap, but I can't because Go is the short-bus of languages
	domainToPartToAddresses := make(map[iface.DomainID]map[iface.PartitionID][]*iface.PartitionServerAddress)

	preferredHosts := []string{}
	var err error

	for _, ring := range p.ringGroup.GetRings() {
		fmt.Println("Building connection cache for ", ring)

		for _, host := range ring.GetHosts(ctx) {
			fmt.Println("Building cache for host: ", host)

			if p.isPreferredHost(ctx, host) {
				preferredHosts = append(preferredHosts, host.GetAddress().Print())
			}

			address := host.GetAddress()
			fmt.Println("Loading partition metadata for Host: ", address)

			for _, hostDomain := range host.GetAssignedDomains(ctx) {

				domain, err := hostDomain.GetDomain(ctx, p.coordinator)
				fmt.Printf("Found assigned %v : %v \n", host.GetAddress().Print(), domain.GetName())

				if err != nil {
					return err
				}

				domainId := domain.GetId()

				if domain == nil {
					return errors.New("Domain not found " + strconv.Itoa(int(domainId)))
				}

				partitionToAddresses := domainToPartToAddresses[domainId]

				if partitionToAddresses == nil {
					partitionToAddresses = make(map[iface.PartitionID][]*iface.PartitionServerAddress)
					domainToPartToAddresses[domainId] = partitionToAddresses
				}

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
			pool := p.serverToConnections[addressStr]
			opts := p.options

			if pool == nil {

				hostConnections := []*HostConnection{}

				fmt.Println("Establishing " + strconv.Itoa(int(opts.NumConnectionsPerHost)) + " connections to " + host.GetAddress().Print() +
					"with connection try lock timeout = " + strconv.Itoa(int(opts.TryLockTimeoutMs)) + "ms, " +
					"connection establisment timeout = " + strconv.Itoa(int(opts.EstablishConnectionTimeoutMs)) + "ms, " +
					"query timeout = " + strconv.Itoa(int(opts.QueryTimeoutMs)) + "ms")

				for i := 1; i <= int(opts.NumConnectionsPerHost); i++ {

					connection := NewHostConnection(
						host,
						opts.TryLockTimeoutMs,
						opts.EstablishConnectionTimeoutMs,
						opts.QueryTimeoutMs,
						opts.BulkQueryTimeoutMs,
					)

					host.AddStateChangeListener(connection)
					hostConnections = append(hostConnections, connection)

				}

				pool, err = CreateHostConnectionPool(hostConnections, NO_SEED, preferredHosts)
				if err != nil {
					return err
				}
			}
			newServerToConnections[addressStr] = pool
		}
	}

	for domainID, connections := range domainToPartToAddresses {

		partitionToConnections := make(map[iface.PartitionID]*HostConnectionPool)

		for partitionID, addresses := range connections {

			connections := []*HostConnection{}
			for _, address := range addresses {
				connections = append(connections, newServerToConnections[address.Print()].GetConnections()...)
			}

			partitionToConnections[partitionID], err = CreateHostConnectionPool(connections,
				getHostListShuffleSeed(domainID, partitionID),
				preferredHosts,
			)
		}

		newDomainToPartitionToConnections[domainID] = partitionToConnections

	}

	return nil
}

func getHostListShuffleSeed(domainId iface.DomainID, partitionId iface.PartitionID) int64 {
	return (int64(domainId) + 1) * (int64(partitionId) + 1)
}
