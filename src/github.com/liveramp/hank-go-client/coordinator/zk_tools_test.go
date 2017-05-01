package coordinator

import (
  "testing"
  "github.com/samuel/go-zookeeper/zk"
  "github.com/curator-go/curator"
  "time"
  "strconv"
  "github.com/stretchr/testify/assert"
  "github.com/liveramp/hank-go-client/test"
  "path"
  "reflect"
)

type logWriter struct {
  t *testing.T
  p string
}

func (lw logWriter) Write(b []byte) (int, error) {
  lw.t.Logf("%s%s", lw.p, string(b))
  return len(b), nil
}

func setup(t *testing.T) (*zk.TestCluster, curator.CuratorFramework) {
  cluster, _ := zk.StartTestCluster(1, nil, logWriter{t: t, p: "[ZKERR] "})
  cluster.StartAllServers()

  client := curator.NewClient("127.0.0.1:"+strconv.Itoa(cluster.Servers[0].Port), curator.NewRetryNTimes(1, time.Second))
  client.Start()

  return cluster, client
}

func TestLocalZkServer(t *testing.T) {
  cluster, client := setup(t)

  conn, _, _ := cluster.ConnectAll()
  conn.Create("/something", []byte("data1"), 0, zk.WorldACL(zk.PermAll))
  get, _, _ := conn.Get("/something")
  assert.Equal(t, "data1", string(get))

  teardown(cluster, client)
}

func TestCurator(t *testing.T) {
  cluster, client := setup(t)

  client.Create().ForPathWithData("/something", []byte("data1"))
  data, _ := client.GetData().ForPath("/something")
  assert.Equal(t, "data1", string(data))

  teardown(cluster, client)
}

func teardown(cluster *zk.TestCluster, client curator.CuratorFramework) {
  client.Close()
  cluster.StopAllServers()
}

func TestZkWatchedNode(t *testing.T) {
  cluster, client := setup(t)

  wn := NewZkWatchedNode(client, "/some/location")
  time.Sleep(time.Second)

  wn.set([]byte("data1"))

  test.WaitUntilOrDie(t, func() bool {
    val, _ := wn.get();
    return string(val) == "data1"
  })

  teardown(cluster, client)

}

type StringValueLoader struct {}

func (p *StringValueLoader) load(path string, client curator.CuratorFramework) interface{} {
  data, _ := client.GetData().ForPath(path)
  return string(data)
}

func TestZkWatchedMap(t *testing.T) {
  cluster, client := setup(t)

  root := "/some/path"

  wmap := NewZkWatchedMap(client, root, &StringValueLoader{})
  time.Sleep(time.Second)

  child1Path := path.Join(root, "child1")

  client.Create().ForPathWithData(child1Path, []byte("data1"))
  test.WaitUntilOrDie(t, func() bool {
    return wmap.Get("child1") == "data1"
  })
  test.WaitUntilOrDie(t, func() bool {
    return reflect.DeepEqual(wmap.KeySet(), []string{"child1"})
  })
  test.WaitUntilOrDie(t, func() bool {
    return reflect.DeepEqual(wmap.Values(), []interface{}{"data1"})
  })

  client.SetData().ForPathWithData(child1Path, []byte("data2"))
  test.WaitUntilOrDie(t, func() bool {
    return wmap.Get("child1") == "data2"
  })

  client.Delete().ForPath(child1Path)
  test.WaitUntilOrDie(t, func() bool {
    return wmap.Get("child1") == nil
  })

  teardown(cluster, client)
}
