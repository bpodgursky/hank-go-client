package coordinator

import (
	"fmt"
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
	root   string
	num    iface.RingID
	client curator.CuratorFramework

	hosts    *watched_structs.ZkWatchedMap
	listener serializers.DataChangeNotifier
}

func loadZkRing(ctx *serializers.ThreadCtx, client curator.CuratorFramework, listener serializers.DataChangeNotifier, root string) (interface{}, error) {
	matches := RING_REGEX.FindStringSubmatch(path.Base(root))

	//  dumb design and rings are directly in the RG root, but can't change it here
	if matches != nil && len(matches) > 0 {

		num, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, err
		}

		hosts, err := watched_structs.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), listener, loadZkHost)
		if err != nil {
			return nil, err
		}

		//  TODO add HostsWatchedMapListener.  it's what tells the client to reload the host cache thing.

		return &ZkRing{root, iface.RingID(num), client, hosts, listener}, nil
	}

	return nil, nil
}

func createZkRing(ctx *serializers.ThreadCtx, root string, num iface.RingID, listener serializers.DataChangeNotifier, client curator.CuratorFramework) (*ZkRing, error) {
	watched_structs.CreateWithParents(client, curator.PERSISTENT, root, nil)

	fmt.Println("Created via creation")

	hosts, err := watched_structs.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), listener, loadZkHost)
	if err != nil {
		return nil, err
	}

	return &ZkRing{root, num, client, hosts, listener}, nil
}

//  public methods

func (p *ZkRing) AddHost(ctx *serializers.ThreadCtx, hostName string, port int, hostFlags []string) (iface.Host, error) {

	host, err := CreateZkHost(ctx, p.client, p.listener, p.hosts.Root, hostName, port, hostFlags)
	if err != nil {
		return nil, err
	}

	//	TODO gross
	err = watched_structs.WaitUntilOrDie(func() bool {
		return p.hosts.Contains(host.GetID())
	})
	if err != nil {
		return nil, err
	}

	p.hosts.Put(host.GetID(), host)

	return host, err
}

func (p *ZkRing) GetHosts(ctx *serializers.ThreadCtx) []iface.Host {

	hosts := []iface.Host{}
	for _, item := range p.hosts.Values() {
		i := item.(iface.Host)
		hosts = append(hosts, i)
	}

	return hosts
}
