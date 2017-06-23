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

	domain, err := coord.AddDomain(ctx, "existent_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, err := coord.AddRingGroup(ctx, "rg1")
	ring1, err := rg1.AddRing(ctx, iface.RingID(0))

	host1, err := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host1Domain, err := host1.AddDomain(ctx, domain)
	host1Domain.AddPartition(ctx, iface.PartitionID(0))

	host2, err := ring1.AddHost(ctx, "127.0.0.1", 12346, []string{})
	fmt.Println(err)

	host2Domain, err := host2.AddDomain(ctx, domain)
	host2Domain.AddPartition(ctx, iface.PartitionID(1))

	dg1, err := coord.AddDomainGroup(ctx, "dg1")

	versions := make(map[iface.DomainID]iface.VersionID)
	versions[domain.GetId()] = iface.VersionID(0)
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
		fmt.Println(partition)
		if partition == 0 {
			server1Values[key] = val
		} else {
			server2Values[key] = val
		}
	}

	fmt.Println("Partition 1: ", server1Values)
	fmt.Println("Partition 2: ", server2Values)

	close1 := createServer(t, ctx, host1, thrift_services.NewPartitionServerHandler(server1Values))
	close2 := createServer(t, ctx, host2, thrift_services.NewPartitionServerHandler(server2Values))

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100)

	smartClient, err := NewHankSmartClient(coord, "rg1", options)

	//	check each record can be found
	for key, value := range values {
		val, _ := smartClient.Get(domain.GetName(), []byte(key))
		assert.Equal(t, value, string(val.Value))
	}

	fakeDomain, _ := smartClient.Get("fake", []byte("na"))
	assert.True(t, reflect.DeepEqual(noSuchDomain(), fakeDomain))

	host1.SetState(ctx, iface.HOST_UPDATING)
	fixtures.WaitUntilOrDie(t, func() bool {
		return host1.GetState() == iface.HOST_UPDATING
	})

	updating, err := smartClient.Get(domain.GetName(), []byte("key1"))

	fmt.Println(updating)

	assert.True(t, reflect.DeepEqual(NoConnectionAvailableResponse(), updating))



	/*
		      // Host is not available
	      host1.setState(HostState.UPDATING);
	      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
	          client.get("existent_domain", KEY_1));

	      // Host is offline but it's the only one, use it opportunistically
	      host2.setState(HostState.OFFLINE);
	      assertEquals(HankResponse.value(VALUE_2), client.get("existent_domain", KEY_2));

	*/

	//assertEquals(HankResponse.xception(HankException.no_such_domain(true)), client.get("nonexistent_domain", null));

	smartClient.Stop()

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}
