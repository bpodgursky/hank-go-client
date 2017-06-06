package hank_client

import (
	"fmt"
	"testing"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/curator-go/curator"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/hank_types"
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

	server, err := createServer(t, ctx, client, 1, &CountingHandler{internal: &ConstValue{val: Val("1")}})

	fixtures.TeardownZookeeper(cluster, client)
}