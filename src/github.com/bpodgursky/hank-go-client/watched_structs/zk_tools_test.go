package watched_structs

import (
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/curator-go/curator"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"path"
	"reflect"
	"testing"
	"time"
	"github.com/bpodgursky/hank-go-client/serializers"
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

	wn, _ := NewZkWatchedNode(client, curator.PERSISTENT, "/some/location", []byte("0"))
	time.Sleep(time.Second)

	wn.Set([]byte("data1"))

	fixtures.WaitUntilOrDie(t, func() bool {
		val, _ := wn.Get()
		return string(val) == "data1"
	})

	fixtures.TeardownZookeeper(cluster, client)

}

func LoadString(ctx *serializers.ThreadCtx, path string, client curator.CuratorFramework) (interface{}, error) {
	data, error := client.GetData().ForPath(path)
	return string(data), error
}

func TestZkWatchedMap(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	root := "/some/path"

	wmap, _ := NewZkWatchedMap(client, root, LoadString)
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

func TestZkWatchedNode2(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	node, _ := NewZkWatchedNode(client, curator.PERSISTENT, "/some/path", []byte("0"))
	node2, _ := NewZkWatchedNode(client, curator.PERSISTENT, "/some/path", []byte("0"))

	testData := "Test String"
  setErr := node.Set([]byte(testData))

  if setErr != nil {
		assert.Fail(t, "Failed")
	}

	fixtures.WaitUntilOrDie(t, func() bool {
		val, _ := node2.Get()
		return reflect.DeepEqual(string(val), testData)
	})

	fixtures.TeardownZookeeper(cluster, client)
}
