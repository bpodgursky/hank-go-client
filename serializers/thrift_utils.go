package serializers

import (
	"sync"
	"git.apache.org/thrift.git/lib/go/thrift"
)

//	TODO probably not the right package name
type DataListener interface {
	OnDataChange(newVal interface{}) error
}

type DataChangeNotifier interface {
	OnChange()
}

type NoOp struct{}

func (t *NoOp) OnDataChange(newVal interface{}) error { return nil }
func (t *NoOp) OnChange()                             {}

type Adapter struct {
	Notifier DataChangeNotifier
}

func (t *Adapter) OnDataChange(newVal interface{}) error {
	t.Notifier.OnChange()
	return nil
}

type MultiNotifier struct {
	clientListeners []DataChangeNotifier
}

func NewMultiNotifier() *MultiNotifier {
	return &MultiNotifier{clientListeners: []DataChangeNotifier{}}
}

func (p *MultiNotifier) AddClient(notifier DataChangeNotifier) {
	p.clientListeners = append(p.clientListeners, notifier)
}

func (p *MultiNotifier) OnChange() {
	for _, listener := range p.clientListeners {
		listener.OnChange()
	}
}

type ThreadCtx struct {
	serializer   *thrift.TSerializer
	deserializer *thrift.TDeserializer

	serializeLock   *sync.Mutex
	deserializeLock *sync.Mutex
}

func NewThreadCtx() *ThreadCtx {

	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocol(serializer.Transport)

	deserializer := thrift.NewTDeserializer()
	deserializer.Protocol = thrift.NewTCompactProtocol(deserializer.Transport)

	return &ThreadCtx{
		serializer:      serializer,
		deserializer:    deserializer,
		serializeLock:   &sync.Mutex{},
		deserializeLock: &sync.Mutex{},
	}

}

type GetBytes func() ([]byte, error)

type SetBytes func(value []byte) error

func (p *ThreadCtx) ReadThrift(get GetBytes, emptyStruct thrift.TStruct) error {

	bytes, err := get()

	if err != nil {
		return err
	}

	return p.ReadThriftBytes(bytes, emptyStruct)
}

func (p *ThreadCtx) ReadThriftBytes(data []byte, emptyStruct thrift.TStruct) error {

	p.deserializeLock.Lock()
	defer p.deserializeLock.Unlock()

	deserErr := p.deserializer.Read(emptyStruct, data)
	if deserErr != nil {
		return deserErr
	}

	return nil
}

func (p *ThreadCtx) SetThrift(set SetBytes, tStruct thrift.TStruct) error {

	bytes, err := p.ToBytes(tStruct)
	if err != nil {
		return err
	}

	return set(bytes)
}

func (p *ThreadCtx) ToBytes(tStruct thrift.TStruct) ([]byte, error) {

	p.serializeLock.Lock()
	defer p.serializeLock.Unlock()

	bytes, err := p.serializer.Write(tStruct)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
