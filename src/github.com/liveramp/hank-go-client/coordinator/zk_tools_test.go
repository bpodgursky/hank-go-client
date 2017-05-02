package coordinator

import (
  "testing"
  "github.com/samuel/go-zookeeper/zk"
  "github.com/curator-go/curator"
  "time"
  "github.com/stretchr/testify/assert"
  "github.com/liveramp/hank-go-client/fixtures"
  "path"
  "reflect"
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

  wn := NewZkWatchedNode(client, "/some/location", true)
  time.Sleep(time.Second)

  wn.Set([]byte("data1"))

  fixtures.WaitUntilOrDie(t, func() bool {
    val, _ := wn.Get();
    return string(val) == "data1"
  })

  fixtures.TeardownZookeeper(cluster, client)

}

type StringValueLoader struct {}

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
