package hank_go_client

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"fmt"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/liveramp/hank-go-client/mock"
)

type HankSmartClient struct {
	connection *zk.Conn

	server *hank.PartitionServer
	transport thrift.TTransport

	sv2 *mock.MapPartitionServerHandler

}


func main() {

	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	children, stat, ch, err := c.ChildrenW("/")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v %+v\n", children, stat)
	e := <-ch
	fmt.Printf("%+v\n", e)
}
