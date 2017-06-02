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
	"github.com/curator-go/curator"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func Exception() *hank.HankResponse {
	resp := &hank.HankResponse{}

	exception := &hank.HankException{}
	exception.InternalError = newStrRef("Internal Error")
	resp.Xception = exception

	return resp
}

func newStrRef(val string) *string {
	return &val
}

type CountingHandler struct {
	numGets          int
	numCompletedGets int

	internal InternalHandler
}

type InternalHandler interface {
	get(int iface.DomainID, key []byte) (*hank.HankResponse, error)
}

type ConstValue struct{ val *hank.HankResponse }

func (p *ConstValue) get(int iface.DomainID, key []byte) (*hank.HankResponse, error) {
	fmt.Println("Query w/ const")
	return p.val, nil
}

type FailingValue struct{}

func (p *FailingValue) get(int iface.DomainID, key []byte) (r *hank.HankResponse, err error) {
	fmt.Println("Query w/ failure")
	return Exception(), nil
}

func (p *CountingHandler) Clear() {
	p.numGets = 0
	p.numCompletedGets = 0
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

func setupCountingServerClient(t *testing.T, ctx *serializers.ThreadCtx, client curator.CuratorFramework, i int) (*CountingHandler, iface.Host, func(), *HostConnection) {
	handler := &CountingHandler{internal: &ConstValue{val: Val1()}}
	host, close, conn := setupServerClient(t, handler, ctx, client, i)
	return handler, host, close, conn
}

func setupFailingServerClient(t *testing.T, ctx *serializers.ThreadCtx, client curator.CuratorFramework, i int) (*CountingHandler, iface.Host, func(), *HostConnection) {
	handler := &CountingHandler{internal: &FailingValue{}}
	host, close, conn := setupServerClient(t, handler, ctx, client, i)
	return handler, host, close, conn
}

func setupServerClient(t *testing.T, server hank.PartitionServer, ctx *serializers.ThreadCtx, client curator.CuratorFramework, i int) (iface.Host, func(), *HostConnection) {
	host, _ := createHost(ctx, client, i)
	_, close := startServer(server, host)

	host.SetState(ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrDie(t, func() bool {
		return host.GetState() == iface.HOST_SERVING
	})

	conn, _ := NewHostConnection(host, 100, 100, 100, 100)

	fixtures.WaitUntilOrDie(t, func() bool {
		return conn.IsServing()
	})

	return host, func() { conn.Disconnect(); close() }, conn
}

func byAddress(connections []*HostConnection) map[string][]*HostConnection {
	hostConnections := make(map[string][]*HostConnection)

	for _, conn := range connections {
		addr := conn.host.GetAddress().Print()

		if _, ok := hostConnections[addr]; !ok {
			hostConnections[addr] = []*HostConnection{}
		}

		hostConnections[addr] = append(hostConnections[addr], conn)
	}

	return hostConnections
}

func setUpCoordinator(client curator.CuratorFramework) (*serializers.ThreadCtx, iface.Domain) {
	ctx := serializers.NewThreadCtx()
	cdr, _ := coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)
	domain, _ := cdr.AddDomain(ctx, "domain1", 1, "", "", "", nil)

	return ctx, domain
}

func TestBothUp(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	//	take one host down, expect all queries on the other

	host2.SetState(ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrDie(t, func() bool {
		return h2conn1.IsOffline()
	})

	handler1.Clear()
	handler2.Clear()

	numHits = queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 0, handler2.numGets)
	assert.Equal(t, 10, numHits)

	//	if both are down, give it a shot anyway

	host1.SetState(ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrDie(t, func() bool {
		return h1conn1.IsOffline()
	})

	handler1.Clear()
	handler2.Clear()
	numHits = 0

	numHits = queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestSimplePreferredPool(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, _, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{host1.GetAddress().Print()})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 0, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestBothPreferred(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{
		host1.GetAddress().Print(),
		host2.GetAddress().Print(),
	})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)

}

func TestPreferredFailing(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupFailingServerClient(t, ctx, client, 1)
	handler2, _, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{host1.GetAddress().Print()})

	//	with 2 attempts, everything should hit first server once, then the other

	numHits := queryKey(pool, domain, t, 10, 2, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 10, handler2.numGets)
	assert.Equal(t, 10, numHits)

	handler1.Clear()
	handler2.Clear()
	numHits = 0

	//	with 1 attempt, everything should hit first server once and fail

	numHits = queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 0, handler2.numGets)
	assert.Equal(t, 0, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestPreferredFallback(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupFailingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)
	handler3, _, close3, h3conn1 := setupCountingServerClient(t, ctx, client, 3)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1, h3conn1}), NO_SEED, []string{
		host1.GetAddress().Print(),
		host2.GetAddress().Print(),
	})

	numHits := queryKey(pool, domain, t, 10, 2, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 10, handler2.numGets)
	assert.Equal(t, 0, handler3.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()
	close3()

	fixtures.TeardownZookeeper(cluster, client)
}

func queryKey(pool *HostConnectionPool, domain iface.Domain, t *testing.T, times int, numTries int32, expected string) int {
	numHits := 0
	for i := 0; i < times; i++ {
		fmt.Println("\n")
		val := pool.Get(domain, Key1(), numTries, NO_HASH)
		if val.IsSetValue() {
			numHits++
		}
	}
	return numHits
}

func createHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, i int) (iface.Host, error) {
	return coordinator.CreateZkHost(ctx, client, "/hank/host/host"+strconv.Itoa(i), "127.0.0.1", 12345+i, []string{})
}

func startServer(handler1 hank.PartitionServer, host iface.Host) (*thrift.TSimpleServer, func()) {
	return thrift_services.Serve(
		handler1,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		host.GetAddress().Print())
}
