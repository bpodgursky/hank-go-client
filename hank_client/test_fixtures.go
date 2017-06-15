package hank_client

import (
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/curator-go/curator"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/thrift_services"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"testing"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"strconv"
)

func createHostServer(t *testing.T, ctx *serializers.ThreadCtx, client curator.CuratorFramework, i int, server hank.PartitionServer) (iface.Host, func()) {
	host, _ := createHost(ctx, client, i)
	return host, createServer(t, ctx, host, server)
}

func createServer(t *testing.T, ctx *serializers.ThreadCtx, host iface.Host, server hank.PartitionServer)(func()){
	_, close := thrift_services.Serve(
		server,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		host.GetAddress().Print())
	host.SetState(ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrDie(t, func() bool {
		return host.GetState() == iface.HOST_SERVING
	})

	return close
}

func createHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, i int) (iface.Host, error) {
	return coordinator.CreateZkHost(ctx, client, "/hank/host/host"+strconv.Itoa(i), "127.0.0.1", 12345+i, []string{})
}
