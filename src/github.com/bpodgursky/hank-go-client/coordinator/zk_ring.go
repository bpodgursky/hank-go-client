package coordinator

import (
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/curator-go/curator"
	"path"
	"regexp"
	"strconv"
)

var RING_REGEX = regexp.MustCompile("ring-([0-9]+)")

const HOSTS_PATH_SEGMENT string = "hosts"

type ZkRing struct {
	root        string
	num         iface.RingID
	client      curator.CuratorFramework
	coordinator *ZkCoordinator

	hosts *watched_structs.ZkWatchedMap
}

func loadZkRing(ctx *serializers.ThreadCtx, client curator.CuratorFramework, root string) (interface{}, error) {
	matches := RING_REGEX.FindStringSubmatch(path.Base(root))

	//  dumb design and rings are directly in the RG root, but can't change it here
	if matches != nil && len(matches) > 0 {

		num, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, err
		}

		hosts, err := watched_structs.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), loadZkHost)
		if err != nil {
			return nil, err
		}

		//  TODO add HostsWatchedMapListener.  it's what tells the client to reload the host cache thing.

		return &ZkRing{root: root, num: iface.RingID(num), client: client, hosts: hosts}, nil
	}

	return nil, nil
}

func createZkRing(ctx *serializers.ThreadCtx, root string, num iface.RingID, client curator.CuratorFramework) (*ZkRing, error) {
	watched_structs.CreateWithParents(client, curator.PERSISTENT, root, nil)

	hosts, err := watched_structs.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), loadZkHost)
	if err != nil {
		return nil, err
	}

	return &ZkRing{root: root, num: num, client: client, hosts: hosts}, nil
}

//  public methods

func (p *ZkRing) AddHost(ctx *serializers.ThreadCtx, hostName string, port int, hostFlags []string) (iface.Host, error) {
	return createZkHost(ctx, p.client, path.Join(p.hosts.Root, HOSTS_PATH_SEGMENT), hostName, port, hostFlags)
}

func (p *ZkRing) GetHosts(ctx *serializers.ThreadCtx) []iface.Host {

	hosts := []iface.Host{}
	for _, item := range p.hosts.Values() {
		i := item.(iface.Host)
		hosts = append(hosts, i)
	}

	return hosts
}
