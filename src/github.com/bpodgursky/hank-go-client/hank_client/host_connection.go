package hank_client

import (
	"errors"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"sync"
	"time"
)

type HostConnection struct {
	host      iface.Host
	hostState iface.HostState

	tryLockTimeoutMs             int32
	establishConnectionTimeoutMs int32
	queryTimeoutMs               int32
	bulkQueryTimeoutMs           int32

	socket *thrift.TSocket
	client *hank.PartitionServerClient

	ctx *serializers.ThreadCtx

	lock *sync.Mutex
}

func NewHostConnection(
	host iface.Host,
	tryLockTimeoutMs int32,
	establishConnectionTimeoutMs int32,
	queryTimeoutMs int32,
	bulkQueryTimeoutMs int32,
) (*HostConnection, error) {

	connection := HostConnection{
		host:                         host,
		tryLockTimeoutMs:             tryLockTimeoutMs,
		establishConnectionTimeoutMs: establishConnectionTimeoutMs,
		queryTimeoutMs:               queryTimeoutMs,
		bulkQueryTimeoutMs:           bulkQueryTimeoutMs,
		lock:                         &sync.Mutex{},
	}

	host.AddStateChangeListener(&connection)

	err := connection.OnDataChange(int(host.GetState()))
	if err != nil {
		return nil, err
	}

	return &connection, nil

}

func (p *HostConnection) disconnect() error {

	var err error

	if p.socket != nil {
		err = p.socket.Close()
	} else {
		err = nil
	}

	p.socket = nil
	p.client = nil

	return err
}

func (p *HostConnection) IsServing() bool {
	return p.hostState == iface.HOST_SERVING
}

func (p *HostConnection) IsOffline() bool {
	return p.hostState == iface.HOST_OFFLINE
}

func (p *HostConnection) IsDisconnected() bool {
	return p.client == nil
}

func (p *HostConnection) Get(id iface.DomainID, key []byte) (*hank.HankResponse, error) {

	defer p.lock.Unlock()

	//	TODO figure out lock timeouts in this fucking trash language
	p.lock.Lock()

	if !p.IsServing() && !p.IsOffline() {
		fmt.Println("returning some bs")
		return nil, errors.New("Connection to host is not available (host is not serving).")
	}

	if p.IsDisconnected() {
		err := p.connect()
		if err != nil {
			p.disconnect()
			return nil, err
		}
	}

	resp, err := p.client.Get(int32(id), key)
	if err != nil {
		p.disconnect()
		return nil, err
	}

	return resp, nil

}

func (p *HostConnection) connect() error {

	p.socket, _ = thrift.NewTSocketTimeout(p.host.GetAddress(p.ctx).Print(), time.Duration(p.establishConnectionTimeoutMs*1e6))
	err := p.socket.Open()
	if err != nil {
		p.disconnect()
		return err
	}

	p.client = hank.NewPartitionServerClientFactory(
		thrift.NewTTransportFactory().GetTransport(p.socket),
		thrift.NewTCompactProtocolFactory())

	err = p.socket.SetTimeout(time.Duration(p.queryTimeoutMs * 1e6))
	if err != nil {
		p.disconnect()
		return err
	}

	return nil
}

func (p *HostConnection) OnDataChange(newVal interface{}) error {

	if newVal == nil {
		newVal = int(iface.HOST_OFFLINE)
	}

	newState := iface.HostState(newVal.(int))

	defer p.lock.Unlock()
	p.lock.Lock()

	disconnectErr := p.disconnect()
	if disconnectErr != nil {
		fmt.Print("Error disconnecting: ", disconnectErr)
	}

	if newState == iface.HOST_SERVING {

		err := p.connect()
		if err != nil {
			fmt.Print("Error connecting to host "+p.host.GetAddress(p.ctx).Print(), err)
			return err
		}

	}

	fmt.Println("updating host state to ", newState)
	p.hostState = newState

	return nil

}
