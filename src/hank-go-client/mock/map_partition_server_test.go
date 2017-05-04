package mock

import (
	"testing"
	"git.apache.org/thrift.git/lib/go/thrift"
	"sync"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

const PARTITION_SERVER_ADDRESS = "127.0.0.1:56783"

func toBytes(str string)(b []byte){
	return []byte(str)
}

func TestMapPartitionServer(t *testing.T) {

	testData := make(map[string]string)

	testData["key1"] = "value1"
	testData["key2"] = "value2"
	testData["key3"] = "value3"

	handler := NewPartitionServerHandler(testData)

	//	set up simple mock thrift partition server
	var wg sync.WaitGroup
	server := Server(
		handler,
		thrift.NewTTransportFactory(),
		thrift.NewTCompactProtocolFactory(),
		PARTITION_SERVER_ADDRESS)

	wg.Add(1)
	go func() {
		server.Serve()
		wg.Done()
	}()

	time.Sleep(time.Second)

	var transport, _ = thrift.NewTSocket(PARTITION_SERVER_ADDRESS)
	transport.Open()

	client := hank.NewPartitionServerClientFactory(
		thrift.NewTTransportFactory().GetTransport(transport),
		thrift.NewTCompactProtocolFactory())

	result, _ := client.Get(0, toBytes("key1"))
	assert.Equal(t, toBytes("value1"), result.Value)

	server.Stop()

	wg.Wait()

}
