package hank_thrift

import (
  "git.apache.org/thrift.git/lib/go/thrift"
  "sync"
)

type ThreadCtx struct {
  serializer   *thrift.TSerializer
  deserializer *thrift.TDeserializer

  serializeLock *sync.Mutex
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

func (p *ThreadCtx) ReadThrift(node WatchedNode, emptyStruct thrift.TStruct) error {

  p.deserializeLock.Lock()
  defer p.deserializeLock.Unlock()

  data, err := node.Get()
  if err != nil {
    return err
  }

  return p.ReadThriftBytes(data, emptyStruct)
}

func (p *ThreadCtx) ReadThriftBytes(data []byte, emptyStruct thrift.TStruct) error{

  deserErr := p.deserializer.Read(emptyStruct, data)
  if deserErr != nil {
    return deserErr
  }

  return nil
}

func (p *ThreadCtx) SetThrift(node WatchedNode, tStruct thrift.TStruct) error {

  p.serializeLock.Lock()
  defer p.serializeLock.Unlock()

  bytes, err := p.serializer.Write(tStruct)
  if err != nil {
    return err
  }

  return node.Set(bytes)
}

type WatchedNode interface {
  Get() ([]byte, error)
  Set(value []byte) (error)
}
