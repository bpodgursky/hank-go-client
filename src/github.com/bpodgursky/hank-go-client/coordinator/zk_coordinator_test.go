package coordinator

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/hank_iface"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"github.com/bpodgursky/hank-go-client/serializers"
)

func TestZkCoordinator(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	zkCoordinator, err1 := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
	zkCoordinator3, err2 := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")

	ctx := serializers.NewThreadCtx()

	if err1 != nil {
		assert.Fail(t, "Error initializing coordinator 1")
	}

	if err2 != nil {
		assert.Fail(t, "Error initializing coordinator 2")
	}

	_, createError := zkCoordinator.AddDomainGroup(ctx, "group1")

	if createError != nil {
		assert.Fail(t, "Error adding domain group")
	}

	//  check the name
	group := zkCoordinator.GetDomainGroup("group1")
	assert.Equal(t, "group1", group.GetName())

	//  make sure this one picked up the message
	fixtures.WaitUntilOrDie(t, func() bool {
		domainGroup := zkCoordinator3.GetDomainGroup("group1")
		return domainGroup != nil
	})

	//  can't create a second one
	_, err := zkCoordinator.AddDomainGroup(ctx, "group1")
	if err == nil {
		assert.Fail(t, "Should have thrown an error")
	}

	//  get the same thing with a fresh coordinator
	zkCoordinator2, _ := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
	group2 := zkCoordinator2.GetDomainGroup("group1")
	assert.Equal(t, "group1", group2.GetName())

	//  verify that rg/rings show up in other coordinators

	rg1Coord1, _ := zkCoordinator.AddRingGroup(ctx, "rg1")

	var rg1Coord2 iface.RingGroup
	fixtures.WaitUntilOrDie(t, func() bool {
		rg1Coord2 = zkCoordinator2.GetRingGroup("rg1")
		return rg1Coord2 != nil
	})

	ringCoord1, _ := rg1Coord1.AddRing(ctx, 0)

	var ringCoord2 iface.Ring
	fixtures.WaitUntilOrDie(t, func() bool {
		ringCoord2 = rg1Coord2.GetRing(0)
		return ringCoord2 != nil
	})

	hostCoord1, _ := ringCoord1.AddHost(ctx, "127.0.0.1", 54321, []string{})

	var hostCoord2 []iface.Host
	fixtures.WaitUntilOrDie(t, func() bool {
		hostCoord2 = ringCoord2.GetHosts(ctx)
		return len(hostCoord2) == 1
	})

	fixtures.WaitUntilOrDie(t, func() bool {
		metadata, _ := hostCoord2[0].GetMetadata(ctx)
		return metadata.HostName == "127.0.0.1"
	})

	fmt.Println(hostCoord1)

	//  let messages flush to make shutdown cleaner.  dunno a better way.
	time.Sleep(time.Second)

	fixtures.TeardownZookeeper(cluster, client)
}
