package coordinator

import (
  "testing"
  "github.com/stretchr/testify/assert"
  "hank-go-client/fixtures"
  "time"
  "hank-go-client/hank_thrift"
)

func TestZkCoordinator(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  zkCoordinator, err1 := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
  zkCoordinator3, err2 := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")

  ctx := hank_thrift.NewThreadCtx()

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


  zkCoordinator.AddRingGroup(ctx, "rg1")

  //  let messages flush to make shutdown cleaner.  dunno a better way.
  time.Sleep(time.Second)

  fixtures.TeardownZookeeper(cluster, client)
}
