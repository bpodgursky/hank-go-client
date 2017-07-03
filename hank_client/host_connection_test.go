package hank_client

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/thrift_services"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
	"github.com/bpodgursky/hank-go-client/zk_coordinator"
	"github.com/bpodgursky/hank-go-client/thriftext"
)

func TestQueryWhenServing(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)
	ctx := thriftext.NewThreadCtx()
	host, err := zk_coordinator.CreateZkHost(ctx, client, &iface.NoOp{}, "/hank/host/host1", "127.0.0.1", 12345, []string{})
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	testData := make(map[string]string)

	testData["key1"] = "value1"
	testData["key2"] = "value2"
	testData["key3"] = "value3"

	handler := thrift_services.NewPartitionServerHandler(testData)

	//	set up simple mock thrift partition server
	_, close := thrift_services.Serve(
		handler,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		"127.0.0.1:12345")

	host.SetState(ctx, iface.HOST_IDLE)

	fixtures.WaitUntilOrDie(t, func() bool {
		return host.GetState() == iface.HOST_IDLE
	})

	conn, _ := NewHostConnection(host, 100, 100, 100, 100)
	_, idleGetErr := conn.Get(0, []byte("key1"), false)

	assert.Equal(t, "Connection to host is not available (host is not serving).", idleGetErr.Error())

	host.SetState(ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrDie(t, func() bool {
		return conn.IsServing()
	})

	resp, err := conn.Get(0, []byte("key1"), false)

	assert.Equal(t, "value1", string(resp.Value))

	conn.Disconnect()
	close()

	fixtures.TeardownZookeeper(cluster, client)
}

type SlowPartitionServerHandler struct{}

func (p *SlowPartitionServerHandler) Get(domain_id int32, key []byte) (r *hank.HankResponse, err error) {
	time.Sleep(time.Second)
	return nil, nil
}

func (p *SlowPartitionServerHandler) GetBulk(domain_id int32, keys [][]byte) (r *hank.HankBulkResponse, err error) {
	time.Sleep(time.Second)
	return nil, nil
}

func TestTimeouts(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := thriftext.NewThreadCtx()
	host, err := zk_coordinator.CreateZkHost(ctx, client, &iface.NoOp{},"/hank/host/host1", "127.0.0.1", 12345, []string{})
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	//	set up simple mock thrift partition server
	_, closeServer := thrift_services.Serve(
		&SlowPartitionServerHandler{},
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		"127.0.0.1:12345")

	host.SetState(ctx, iface.HOST_SERVING)

	conn, _ := NewHostConnection(host, 100, 100, 100, 100)

	fixtures.WaitUntilOrDie(t, func() bool {
		return conn.IsServing()
	})

	_, err = conn.Get(0, []byte("key1"), false)

	assert.True(t, strings.Contains(err.Error(), "i/o timeout"))

	conn.Disconnect()
	closeServer()

	fixtures.TeardownZookeeper(cluster, client)
}
