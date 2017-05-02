package coordinator

import (
  "testing"
  "github.com/liveramp/hank-go-client/fixtures"
  "github.com/stretchr/testify/assert"
  "fmt"
)

func TestZkCoordinator(t *testing.T){
  cluster, client := fixtures.SetupZookeeper(t)

  //  TODO try with https://github.com/curator-go/curator/pull/5

  zkCoordinator, err1 := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
  zkCoordinator3, err2 := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")

  if err1 != nil{
    assert.Fail(t, "Error initializing coordinator 1")
  }

  if err2 != nil {
    assert.Fail(t, "Error initializing coordinator 2")
  }

  _, createError := zkCoordinator.addDomainGroup("group1")

  if createError != nil {
    fmt.Println(createError)
    assert.Fail(t, "Error adding domain group")
  }

  //  check the name
  group := zkCoordinator.getDomainGroup("group1")
  assert.Equal(t, "group1", group.getName())

  //  make sure this one picked up the message
  fixtures.WaitUntilOrDie(t, func() bool{
    domainGroup := zkCoordinator3.getDomainGroup("group1")
    fmt.Println("waiting on", domainGroup)
    return domainGroup != nil
  })

  //  can't create a second one
  _, err := zkCoordinator.addDomainGroup("group1")
  if err == nil {
   assert.Fail(t, "Should have thrown an error")
  }

  //  get the same thing with a fresh coordinator
  zkCoordinator2, _ := NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
  group2 := zkCoordinator2.getDomainGroup("group1")
  assert.Equal(t, "group1", group2.getName())

  fixtures.TeardownZookeeper(cluster, client)
}

