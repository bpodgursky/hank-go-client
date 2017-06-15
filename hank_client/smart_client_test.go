package hank_client

import (
	"fmt"
	"testing"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
)

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
		SetNumConnectionsPerHost(2).
		Build()

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

	coordinator, _ := coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)

	domain, err := coordinator.AddDomain(ctx, "existent_domain", 2, "", "", "", []string{})

	rg1, err := coordinator.AddRingGroup(ctx, "rg1")
	ring1, err := rg1.AddRing(ctx, iface.RingID(0))

	host1, err := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host1Domain, err := host1.AddDomain(ctx, domain)
	host1Domain.AddPartition(ctx, iface.PartitionID(0))

	host2, err := ring1.AddHost(ctx, "localhost", 12346, []string{})
	host2Domain, err := host2.AddDomain(ctx, domain)
	host2Domain.AddPartition(ctx, iface.PartitionID(1))

	dg1, err := coordinator.AddDomainGroup(ctx, "dg1")

	versions := make(map[iface.DomainID]iface.VersionID)
	versions[domain.GetId()] = iface.VersionID(0)
	dg1.SetDomainVersions(ctx, versions)

	close1 := createServer(t, ctx, host1, &CountingHandler{internal: &ConstValue{val: Val("1")}})
	close2 := createServer(t, ctx, host2, &CountingHandler{internal: &ConstValue{val: Val("2")}})

	fixtures.WaitUntilOrDie(t, func() bool {
		return host1.GetState() == iface.HOST_SERVING && host2.GetState() == iface.HOST_SERVING
	})

	//	TODO test

	close1()
	close2()

	fmt.Println(err)

	fixtures.TeardownZookeeper(cluster, client)
}
