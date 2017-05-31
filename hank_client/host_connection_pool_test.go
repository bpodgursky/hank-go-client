package hank_client

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/thrift_services"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type CountingHandler struct {
	numGets          int
	numCompletedGets int

	internal InternalHandler
}

type InternalHandler interface {
	get(int iface.DomainID, key []byte) (*hank.HankResponse, error)
}

type ConstValue struct {
	val *hank.HankResponse
}

func (p *ConstValue) get(int iface.DomainID, key []byte) (*hank.HankResponse, error) {
	return p.val, nil
}

func (p *CountingHandler) Get(domain_id int32, key []byte) (r *hank.HankResponse, err error) {

	p.numGets++
	response, err := p.internal.get(iface.DomainID(domain_id), key)

	if err == nil {
		p.numCompletedGets++
	} else {
		return nil, err
	}

	return response, nil

}
func (p *CountingHandler) GetBulk(domain_id int32, keys [][]byte) (r *hank.HankBulkResponse, err error) {
	return nil, nil
}

const Val1Str = "1"

func Val1() *hank.HankResponse {
	resp := &hank.HankResponse{}
	resp.Value = []byte(Val1Str)
	resp.NotFound = newFalse()
	return resp
}

func newFalse() *bool {
	b := false
	return &b
}

func Key1() []byte {
	return []byte("1")
}

func TestBothUp(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	//	set up simple mock thrift partition server
	handler1 := &CountingHandler{internal: &ConstValue{val: Val1()}}
	handler2 := &CountingHandler{internal: &ConstValue{val: Val1()}}

	ctx := serializers.NewThreadCtx()

	cdr, _ := coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)
	domain, _ := cdr.AddDomain(ctx, "domain1", 1, "", "", "", nil)

	host1, _ := coordinator.CreateZkHost(ctx, client, "/hank/host/host1", "127.0.0.1", 12345, []string{})
	host2, _ := coordinator.CreateZkHost(ctx, client, "/hank/host/host2", "127.0.0.1", 12346, []string{})

	startServer(handler1, host1)
	startServer(handler2, host2)

	host1.SetState(ctx, iface.HOST_SERVING)
	host2.SetState(ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrDie(t, func() bool {
		return host1.GetState() == iface.HOST_SERVING && host2.GetState() == iface.HOST_SERVING
	})

	h1conn1, _ := NewHostConnection(host1, 100, 100, 100, 100)
	h2conn1, _ := NewHostConnection(host2, 100, 100, 100, 100)

	fixtures.WaitUntilOrDie(t, func() bool {
		return h1conn1.IsServing() && h2conn1.IsServing()
	})

	hostConnections := make(map[string][]*HostConnection)

	hostConnections[host1.GetAddress().Print()] = []*HostConnection{h1conn1}
	hostConnections[host2.GetAddress().Print()] = []*HostConnection{h2conn1}

	pool, _ := NewHostConnectionPool(hostConnections, NO_SEED, []string{})
	numHits := 0

	for i := 0; i < 10; i++ {
		val := pool.Get(domain, Key1(), 1, NO_HASH)
		assert.Equal(t, Val1Str, string(val.Value))
		if val.IsSetValue() {
			numHits++
		}
	}

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	fixtures.TeardownZookeeper(cluster, client)
}

func startServer(handler1 *CountingHandler, host iface.Host) (*thrift.TSimpleServer, *sync.WaitGroup) {
	var wg sync.WaitGroup
	server := thrift_services.Server(
		handler1,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		host.GetAddress().Print())
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		server.Serve()
		wg.Done()
	}(&wg)

	return server, &wg
}
