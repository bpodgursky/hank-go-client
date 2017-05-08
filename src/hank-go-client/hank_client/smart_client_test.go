package hank_client

import (
  "testing"
  "hank-go-client/coordinator"
  "hank-go-client/fixtures"
  "fmt"
  "hank-go-client/hank_thrift"
  "github.com/stretchr/testify/assert"
)

func TestSmartClient(t *testing.T) {

  cluster, client := fixtures.SetupZookeeper(t)

  ctx := hank_thrift.NewThreadCtx()

  coordinator, _ := coordinator.NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
  rg, _ := coordinator.AddRingGroup(ctx,"group1")

  fixtures.WaitUntilOrDie(t, func() bool{
    return coordinator.GetRingGroup("group1") != nil
  })

  smartClient, _ := NewHankSmartClient(coordinator, "group1")
  smartClient2, _ := NewHankSmartClient(coordinator, "group1")

  ret := fixtures.WaitUntilOrDie(t, func() bool {
    return len(rg.GetClients()) == 2
  })

  assert.Nil(t, ret)

  fmt.Println(smartClient)
  fmt.Println(smartClient2)

  fixtures.TeardownZookeeper(cluster, client)
}
