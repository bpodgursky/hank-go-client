package hank_client

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/serializers"
	"testing"
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
