package coordinator

import "github.com/curator-go/curator"

type ZkRingGroup struct {
  ringGroupPath string
}

type ZkRingGroupLoader struct {}

func (p *ZkRingGroupLoader) load(path string, client curator.CuratorFramework) (interface{}, error) {
  return &ZkRingGroup{ringGroupPath:path}, nil
}
