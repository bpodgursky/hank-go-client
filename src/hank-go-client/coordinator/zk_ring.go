package coordinator

import (
  "hank-go-client/hank_thrift"
  "github.com/curator-go/curator"
  "hank-go-client/hank_zk"
  "regexp"
  "path"
  "strconv"
  "hank-go-client/hank_iface"
)

var RING_REGEX = regexp.MustCompile("ring-([0-9]+)")

const HOSTS_PATH_SEGMENT string = "hosts"

type ZkRing struct {
  root   string
  num    int
  client curator.CuratorFramework

  hosts *hank_zk.ZkWatchedMap
}

func loadZkRing(ctx *hank_thrift.ThreadCtx, root string, client curator.CuratorFramework) (interface{}, error) {
  matches := RING_REGEX.FindStringSubmatch(path.Base(root))

  //  dumb design and rings are directly in the RG root, but can't change it here
  if matches != nil && len(matches) > 0{

    num, err := strconv.Atoi(matches[1])
    if err != nil {
      return nil, err
    }

    hosts, err := hank_zk.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), loadZkHost)
    if err != nil{
      return nil, err
    }

    return &ZkRing{root: root, num:  num, client: client, hosts: hosts}, nil
  }

  return nil, nil
}

func createZkRing(ctx *hank_thrift.ThreadCtx, root string, num int, client curator.CuratorFramework) (*ZkRing, error) {
  hank_zk.CreateWithParents(client, curator.PERSISTENT, root, nil)

  hosts, err := hank_zk.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), loadZkHost)
  if err != nil{
    return nil, err
  }

  return &ZkRing{root: root, num: num, client: client, hosts: hosts}, nil
}

//  public methods

func (p *ZkRing) AddHost(ctx *hank_thrift.ThreadCtx, hostName string, port int, hostFlags []string) (hank_iface.Host, error){
  return createZkHost(ctx, p.client, path.Join(p.hosts.Root, HOSTS_PATH_SEGMENT), hostName, port, hostFlags)
}

func (p *ZkRing)  GetHosts(ctx *hank_thrift.ThreadCtx) []hank_iface.Host {

  hosts := []hank_iface.Host{}
  for _, item := range p.hosts.Values() {
    i := item.(hank_iface.Host)
    hosts = append(hosts, i)
  }

  return hosts
}
