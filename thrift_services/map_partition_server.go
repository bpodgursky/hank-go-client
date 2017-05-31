package thrift_services

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/hank_types"
)

type MapPartitionServerHandler struct {
	mockData map[string]string
}

func NewPartitionServerHandler(mockData map[string]string) *MapPartitionServerHandler {
	return &MapPartitionServerHandler{mockData: mockData}
}

//	assume everything is in one domain for testing
func (p *MapPartitionServerHandler) Get(domain_id int32, key []byte) (r *hank.HankResponse, err error) {
	var response = hank.NewHankResponse()
	response.Value = []byte(p.mockData[string(key)])
	response.NotFound = newFalse()
	response.Xception = nil
	return response, nil
}

func newFalse() *bool {
	b := false
	return &b
}

func (p *MapPartitionServerHandler) GetBulk(domain_id int32, keys [][]byte) (r *hank.HankBulkResponse, err error) {

	var response = hank.NewHankBulkResponse()
	var responses = make([]*hank.HankResponse, 0)

	for _, element := range keys {
		v, _ := p.Get(0, element)
		responses = append(responses, v)
	}

	response.Responses = responses
	return response, nil

}

func Server(
	handler hank.PartitionServer,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory,
	addr string) *thrift.TSimpleServer {

	var transport, _ = thrift.NewTServerSocket(addr)

	fmt.Printf("%T\n", transport)
	processor := hank.NewPartitionServerProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Starting the simple server... on ", addr)

	return server
}
