package hank_zk

import (
  "testing"
  "github.com/samuel/go-zookeeper/zk"
  "github.com/curator-go/curator"
  "time"
  "github.com/stretchr/testify/assert"
  "path"
  "reflect"
  "hank-go-client/fixtures"
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "hank-go-client/hank_thrift"
  "hank-go-client/hank_util"
)

func TestLocalZkServer(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  conn, _, _ := cluster.ConnectAll()
  conn.Create("/something", []byte("data1"), 0, zk.WorldACL(zk.PermAll))
  get, _, _ := conn.Get("/something")
  assert.Equal(t, "data1", string(get))

  fixtures.TeardownZookeeper(cluster, client)
}

func TestCurator(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  client.Create().ForPathWithData("/something", []byte("data1"))
  data, _ := client.GetData().ForPath("/something")
  assert.Equal(t, "data1", string(data))

  fixtures.TeardownZookeeper(cluster, client)
}

func TestZkWatchedNode(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  wn := NewZkWatchedNode(client, "/some/location")
  time.Sleep(time.Second)

  wn.Set([]byte("data1"))

  fixtures.WaitUntilOrDie(t, func() bool {
    val, _ := wn.Get();
    return string(val) == "data1"
  })

  fixtures.TeardownZookeeper(cluster, client)

}

type StringValueLoader struct{}

func (p *StringValueLoader) load(path string, client curator.CuratorFramework) (interface{}, error) {
  data, error := client.GetData().ForPath(path)
  return string(data), error
}

func TestZkWatchedMap(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  root := "/some/path"

  wmap, _ := NewZkWatchedMap(client, root, &StringValueLoader{})
  time.Sleep(time.Second)

  child1Path := path.Join(root, "child1")

  client.Create().ForPathWithData(child1Path, []byte("data1"))
  fixtures.WaitUntilOrDie(t, func() bool {
    return wmap.Get("child1") == "data1"
  })
  fixtures.WaitUntilOrDie(t, func() bool {
    return reflect.DeepEqual(wmap.KeySet(), []string{"child1"})
  })
  fixtures.WaitUntilOrDie(t, func() bool {
    return reflect.DeepEqual(wmap.Values(), []interface{}{"data1"})
  })

  client.SetData().ForPathWithData(child1Path, []byte("data2"))
  fixtures.WaitUntilOrDie(t, func() bool {
    return wmap.Get("child1") == "data2"
  })

  client.Delete().ForPath(child1Path)
  fixtures.WaitUntilOrDie(t, func() bool {
    return wmap.Get("child1") == nil
  })

  fixtures.TeardownZookeeper(cluster, client)
}

func TestZkWatchedThriftNode(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  node := NewZkWatchedNode(client, "/some/path")
  node2 := NewZkWatchedNode(client, "/some/path")

  testData := hank.NewDomainGroupMetadata()
  testData.DomainVersions = make(map[int32]int32)
  testData.DomainVersions[0] = 1

  ctx := hank_thrift.NewThreadCtx()
  set := hank_thrift.SetThrift(ctx, node, testData)

  if set != nil{
    assert.Fail(t, "Failed")
  }

  fixtures.WaitUntilOrDie(t, func() bool{
    val, _ := hank_util.GetDomainGroupMetadata(ctx, node2)
    return reflect.DeepEqual(val, testData)
  })

  fixtures.TeardownZookeeper(cluster, client)
}