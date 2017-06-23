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
	"sync"
	"github.com/hashicorp/golang-lru"
	"math"
)

type RequestCounters struct {
	requests  int64
	cacheHits int64

	lock *sync.Mutex
}

func NewRequestCounters() (*RequestCounters) {
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

	cache    *lru.Cache
	counters *RequestCounters

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

	var cache *lru.Cache

	if options.ResponseCacheEnabled {
		cache, err = lru.New(int(options.ResponseCacheNumItems))
	}

	client := &HankSmartClient{coordinator,
														 ringGroup,
														 options,
														 make(map[string]*iface.PartitionServerAddress),
														 make(map[string]*HostConnectionPool),
														 make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool),
														 &sync.Mutex{},
														 cache,
														 NewRequestCounters(),
	}

	client.updateConnectionCache(ctx)

	ringGroup.AddListener(client)

	return client, nil
}

func (p *HankSmartClient) OnDataChange() {


	//	TODO implement



	//sdfa

}

func (p *HankSmartClient) updateLoop(stopping *bool, listenerLock *SingleLockSemaphore) {

	ctx := serializers.NewThreadCtx()

	for !(*stopping) {
	 //p.listenerLock.Read()
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

func (p *HankSmartClient) updateConnectionCache(ctx *serializers.ThreadCtx) {
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
	key    []byte
}

func (p *HankSmartClient) get(domain iface.Domain, key []byte) (*hank.HankResponse, error) {

	if key == nil {
		return nil, errors.New("Null key")
	}

	if len(key) == 0 {
		return nil, errors.New("Empty key")
	}

	domainID := domain.GetId()
	entry := Entry{domainID, key}

	var val interface{}
	var ok bool

	if p.cache != nil {
		val, ok = p.cache.Get(entry)
	}

	if ok {
		p.counters.increment(1, 1)
		return val.(*hank.HankResponse), nil
	} else {
		p.counters.increment(1, 0)

		// Determine HostConnectionPool to use
		partitioner := domain.GetPartitioner()
		partition := partitioner.Partition(key, domain.GetNumParts())
		keyHash := partitioner.Partition(key, math.MaxInt32)

		p.connectionLock.Lock()
		pools := p.domainToPartToConnections[domainID]
		p.connectionLock.Unlock()

		fmt.Println(p.domainToPartToConnections)

		if pools == nil {
			fmt.Printf("Could not find domain to partition map for domain %v (id: %v)]\n", domain.GetName(), domainID)
			return noReplica(), nil
		}

		pool := pools[iface.PartitionID(partition)]

		if pool == nil {
			fmt.Printf("Could not find list of hosts for domain %v, partition %v\n", domain.GetName(), partition)
			return noReplica(), nil
		}

		response := pool.Get(domain, key, p.options.QueryMaxNumTries, keyHash)

		if p.cache != nil && response != nil && (response.IsSetNotFound() || response.IsSetValue()) {
			p.cache.Add(key, response)
		}

		if response.IsSetXception() {
			fmt.Printf("Failed to perform get: domain: %v partition; %v key; %v", domain, partition, key)
		}

		return response, nil

	}

}

func (p *HankSmartClient) isPreferredHost(ctx *serializers.ThreadCtx, host iface.Host) bool {

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
	ctx *serializers.ThreadCtx,
	newServerToConnections map[string]*HostConnectionPool,
	newDomainToPartitionToConnections map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool) error {

	//  this is horrible looking, and I'd love to use MultiMap, but I can't  because this horseshit,
	//  gimp, special-ed language thinks that generics are too dangerous and just gives you fucking crayons
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
			pool := p.serverToConnections[addressStr]
			opts := p.options

			fmt.Println(opts)

			if pool == nil {

				hostConnections := []*HostConnection{}

				fmt.Println("Establishing " + strconv.Itoa(int(opts.NumConnectionsPerHost)) + " connections to " + host.GetAddress().Print() +
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
					} else {
						host.AddStateChangeListener(connection)
						hostConnections = append(hostConnections, connection)
					}

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

func getHostListShuffleSeed(domainId iface.DomainID, partitionId iface.PartitionID) int32 {
	return (int32(domainId) + 1) * (int32(partitionId) + 1)
}
