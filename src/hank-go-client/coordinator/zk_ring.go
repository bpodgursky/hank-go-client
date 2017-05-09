package coordinator

import (
  "hank-go-client/hank_thrift"
  "github.com/curator-go/curator"
  "hank-go-client/hank_zk"
  "regexp"
  "path"
  "strconv"
)

var RING_REGEX = regexp.MustCompile("ring-([0-9]+)")

type ZkRing struct {
  root   string
  num    int
  client curator.CuratorFramework
}

func loadZkRing(ctx *hank_thrift.ThreadCtx, root string, client curator.CuratorFramework) (interface{}, error) {
  matches := RING_REGEX.FindStringSubmatch(path.Base(root))

  //  dumb design and rings are directly in the RG root, but can't change it here
  if matches != nil && len(matches) > 0{

    num, err := strconv.Atoi(matches[1])
    if err != nil {
      return nil, err
    }

    return &ZkRing{root: root, num:  num, client: client}, nil
  }

  return nil, nil
}

func createZkRing(ctx *hank_thrift.ThreadCtx, root string, num int, client curator.CuratorFramework) (*ZkRing, error) {
  hank_zk.CreateWithParents(client, curator.PERSISTENT, root, nil)
  return &ZkRing{root: root, num: num, client: client}, nil
}
