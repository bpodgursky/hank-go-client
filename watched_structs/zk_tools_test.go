package watched_structs

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/curator-go/curator"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"path"
	"reflect"
	"testing"
	"time"
  "github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/hank_types"
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

	wn, err := NewBytesWatchedNode(client, curator.PERSISTENT,
		"/some/location",
		[]byte("0"),
	)

	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	ctx := serializers.NewThreadCtx()

	time.Sleep(time.Second)

	wn.Set(ctx, []byte("data1"))

	fixtures.WaitUntilOrDie(t, func() bool {
		val, err := wn.Get().([]byte)

		fmt.Println(val)
		fmt.Println(err)

		return string(val) == "data1"
	})

	fixtures.TeardownZookeeper(cluster, client)

}

func LoadString(ctx *serializers.ThreadCtx, client curator.CuratorFramework, path string) (interface{}, error) {
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

	node, _ := NewBytesWatchedNode(client, curator.PERSISTENT, "/some/path", []byte("0"))
	node2, _ := LoadBytesWatchedNode(client, "/some/path")

	ctx := serializers.NewThreadCtx()

	testData := "Test String"
	setErr := node.Set(ctx, []byte(testData))

	if setErr != nil {
		assert.Fail(t, "Failed")
	}

	fixtures.WaitUntilOrDie(t, func() bool {
		val := asBytes(node2.Get())
		if val != nil {
			return reflect.DeepEqual(string(val), testData)
		}
		return false
	})

	fixtures.TeardownZookeeper(cluster, client)
}

func TestUpdateWatchedNode(t *testing.T) {
  cluster, client := fixtures.SetupZookeeper(t)

  hostData := hank.NewHostAssignmentsMetadata()
  hostData.Domains = make(map[int32]*hank.HostDomainMetadata)
  hostData.Domains[0] = hank.NewHostDomainMetadata()

  ctx := serializers.NewThreadCtx()

  node, _ := NewThriftWatchedNode(client, curator.PERSISTENT, "/some/path", ctx, iface.NewHostAssignmentMetadata, hostData)
  node2, _ := LoadThriftWatchedNode(client, "/some/path", iface.NewHostAssignmentMetadata)

  node.Update(ctx, func(val interface{}) interface{} {
    meta := val.(*hank.HostAssignmentsMetadata)
    meta.Domains[1] = hank.NewHostDomainMetadata()
    return meta
  })

  fixtures.WaitUntilOrDie(t, func() bool {
    meta := iface.AsHostAssignmentsMetadata(node2.Get())
    return meta != nil && len(meta.Domains) == 2
  })

  fixtures.TeardownZookeeper(cluster, client)
}

func asBytes(val interface{}) []byte {
	if val != nil {
		return val.([]byte)
	}
	return nil
}
