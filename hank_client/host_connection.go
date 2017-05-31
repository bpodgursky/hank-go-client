package hank_client

import (
	"errors"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/hank_types"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
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

	lock *TimeoutMutex
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
		lock:                         NewMutex(),
	}

	host.AddStateChangeListener(&connection)

	err := connection.OnDataChange(string(host.GetState()))
	if err != nil {
		return nil, err
	}

	return &connection, nil

}

func (p *HostConnection) Disconnect() error {

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

func (p *HostConnection) TryImmediateLock() bool {
	return p.lock.TryLockNoWait()
}

func (p *HostConnection) TryLockWithTimeout() bool {

	if p.tryLockTimeoutMs == 0 {
		p.lock.Lock()
		return true
	}

	return p.lock.TryLock(time.Duration(p.tryLockTimeoutMs) * time.Millisecond)
}

func (p *HostConnection) Lock() {
	p.lock.Lock()
}

func (p *HostConnection) Unlock() {
	p.lock.Unlock()
}

//	this is NOT threadsafe.  since Golang doesn't have reentrant locks b/c it is too primitive, acquire the locks
//	yourself.
func (p *HostConnection) Get(id iface.DomainID, key []byte) (*hank.HankResponse, error) {

	if !p.IsServing() && !p.IsOffline() {
		return nil, errors.New("Connection to host is not available (host is not serving).")
	}

	if p.IsDisconnected() {
		err := p.connect()
		if err != nil {
			p.Disconnect()
			return nil, err
		}
	}

	resp, err := p.client.Get(int32(id), key)

	if err != nil {
		p.Disconnect()
		return nil, err
	}

	return resp, nil

}

func (p *HostConnection) connect() error {

	p.socket, _ = thrift.NewTSocketTimeout(p.host.GetAddress().Print(), time.Duration(p.establishConnectionTimeoutMs*1e6))
	framed := thrift.NewTFramedTransportMaxLength(p.socket, 16384000)

	err := framed.Open()
	if err != nil {
		fmt.Println(err)
		p.Disconnect()
		return err
	}

	p.client = hank.NewPartitionServerClientFactory(
		framed,
		thrift.NewTCompactProtocolFactory(),
	)

	err = p.socket.SetTimeout(time.Duration(p.queryTimeoutMs * 1e6))
	if err != nil {
		p.Disconnect()
		return err
	}

	return nil
}

func (p *HostConnection) OnDataChange(newVal interface{}) error {

	if newVal == nil {
		newVal = string(iface.HOST_OFFLINE)
	}

	newState := iface.HostState(newVal.(string))

	defer p.Unlock()
	p.Lock()

	disconnectErr := p.Disconnect()
	if disconnectErr != nil {
		fmt.Print("Error disconnecting: ", disconnectErr)
	}

	if newState == iface.HOST_SERVING {

		err := p.connect()
		if err != nil {
			fmt.Print("Error connecting to host "+p.host.GetAddress().Print(), err)
			return err
		}

	}

	p.hostState = newState

	return nil

}
