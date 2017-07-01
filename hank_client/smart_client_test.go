package hank_client

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/thrift_services"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"math/big"
	"reflect"
	"testing"
)

func TestAsdf(t *testing.T) {

	uuid := uuid.NewV4().Bytes()
	last := uuid[len(uuid)-8:]

	var number big.Int
	number.SetBytes(last)
	fmt.Println(number.Int64())

}

func TestSmartClient(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := serializers.NewThreadCtx()

	coordinator, _ := coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)

	rg, _ := coordinator.AddRingGroup(ctx, "group1")

	fixtures.WaitUntilOrDie(t, func() bool {
		return coordinator.GetRingGroup("group1") != nil
	})

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2)

	smartClient, _ := NewHankSmartClient(coordinator, "group1", options)
	smartClient2, _ := NewHankSmartClient(coordinator, "group1", options)

	fixtures.WaitUntilOrDie(t, func() bool {
		return len(rg.GetClients()) == 2
	})

	ring, _ := rg.AddRing(ctx, 0)
	host, _ := ring.AddHost(ctx, "127.0.0.1", 54321, []string{})

	fmt.Println(ring)
	fmt.Println(host)

	fmt.Println(smartClient)
	fmt.Println(smartClient2)

	fixtures.TeardownZookeeper(cluster, client)
}

func Val(val string) *hank.HankResponse {
	resp := &hank.HankResponse{}
	resp.Value = []byte(val)
	resp.NotFound = newFalse()
	return resp
}

func TestIt(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := serializers.NewThreadCtx()

	coord, _ := coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)

	domain0, err := coord.AddDomain(ctx, "existent_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, err := coord.AddRingGroup(ctx, "rg1")
	ring1, err := rg1.AddRing(ctx, iface.RingID(0))

	host0, err := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, err := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	host1, err := ring1.AddHost(ctx, "127.0.0.1", 12346, []string{})
	fmt.Println(err)

	host1Domain, err := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(1))

	dg1, err := coord.AddDomainGroup(ctx, "dg1")

	versions := make(map[iface.DomainID]iface.VersionID)
	versions[domain0.GetId()] = iface.VersionID(0)
	dg1.SetDomainVersions(ctx, versions)

	partitioner := &coordinator.Murmur64Partitioner{}

	values := make(map[string]string)
	values["key1"] = "value1"
	values["key2"] = "value2"
	values["key3"] = "value3"
	values["key4"] = "value4"
	values["key5"] = "value5"
	values["key6"] = "value6"
	values["key7"] = "value7"
	values["key8"] = "value8"
	values["key9"] = "value9"
	values["key0"] = "value0"

	server1Values := make(map[string]string)
	server2Values := make(map[string]string)

	for key, val := range values {
		partition := partitioner.Partition([]byte(key), 2)
		fmt.Printf("%v => %v\n", key, partition)
		if partition == 0 {
			server1Values[key] = val
		} else {
			server2Values[key] = val
		}
	}

	handler1 := thrift_services.NewPartitionServerHandler(server1Values)
	handler12 := thrift_services.NewPartitionServerHandler(server2Values)

	close1 := createServer(t, ctx, host0, handler1)
	close2 := createServer(t, ctx, host1, handler12)

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100)

	smartClient, err := NewHankSmartClient(coord, "rg1", options)

	//	check each record can be found
	for key, value := range values {
		val, _ := smartClient.Get(domain0.GetName(), []byte(key))
		assert.Equal(t, value, string(val.Value))
	}

	fakeDomain, _ := smartClient.Get("fake", []byte("na"))
	assert.True(t, reflect.DeepEqual(noSuchDomain(), fakeDomain))

	//	no replicas live if updating
	setStateBlocking(t, host1, ctx, iface.HOST_UPDATING)
	fixtures.WaitUntilOrDie(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual(NoConnectionAvailableResponse(), updating)
	})

	//	when offline, try anyway if it's the only replica
	setStateBlocking(t, host1, ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrDie(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	ok again when serving
	setStateBlocking(t, host1, ctx, iface.HOST_SERVING)
	fixtures.WaitUntilOrDie(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	test when a new domain is added, the client picks it up
	domain1, err := coord.AddDomain(ctx, "second_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	//	assign partitions to it
	host1Domain1, err := host1.AddDomain(ctx, domain1)
	host1Domain1.AddPartition(ctx, iface.PartitionID(1))
	fixtures.WaitUntilOrDie(t, func() bool {
		return len(host1.GetAssignedDomains(ctx)) == 2
	})

	//	verify that the client can query the domain now
	fixtures.WaitUntilOrDie(t, func() bool {
		updating, _ := smartClient.Get(domain1.GetName(), []byte("key1"))
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	test caching

	handler12.ClearRequestCounters()

	cachingOptions := NewHankSmartClientOptions().
		SetResponseCacheEnabled(true).
		SetResponseCacheNumItems(10).
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100)

	cachingClient, err := NewHankSmartClient(coord, "rg1", cachingOptions)

	//	query once
	val, err := cachingClient.Get(domain1.GetName(), []byte("key1"))
	assert.True(t, reflect.DeepEqual("value1", string(val.Value)))
	assert.Equal(t, int32(1), handler12.NumRequests)

	// verify was found in cache
	val, err = cachingClient.Get(domain1.GetName(), []byte("key1"))
	assert.True(t, reflect.DeepEqual("value1", string(val.Value)))
	assert.Equal(t, int32(1), handler12.NumRequests)

	//	TODO cache expiry

	//	test adding a new server and taking one of the original ones down

	setStateBlocking(t, host1, ctx, iface.HOST_UPDATING)

	host2, err := ring1.AddHost(ctx, "localhost", 12347, []string{})
	host2Domain, err := host2.AddDomain(ctx, domain0)
	host2Domain.AddPartition(ctx, iface.PartitionID(1))

	handler2 := thrift_services.NewPartitionServerHandler(server2Values)
	close3 := createServer(t, ctx, host2, handler2)

	//	make server 1 unreachable
	fixtures.WaitUntilOrDie(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		fmt.Println(updating)
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	smartClient.Stop()
	cachingClient.Stop()

	close1()
	close2()
	close3()

	fixtures.TeardownZookeeper(cluster, client)
}

func setStateBlocking(t *testing.T, host iface.Host, ctx *serializers.ThreadCtx, state iface.HostState) {
	host.SetState(ctx, state)
	fixtures.WaitUntilOrDie(t, func() bool {
		return host.GetState() == state
	})
}
