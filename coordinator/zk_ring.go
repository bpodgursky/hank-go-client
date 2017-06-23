package coordinator

import (
	"github.com/curator-go/curator"
	"path"
	"regexp"
	"strconv"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/watched_structs"
	"github.com/bpodgursky/hank-go-client/serializers"
)

var RING_REGEX = regexp.MustCompile("ring-([0-9]+)")

const HOSTS_PATH_SEGMENT string = "hosts"

type ZkRing struct {
	root        string
	num         iface.RingID
	client      curator.CuratorFramework
	coordinator *ZkCoordinator

	hosts *watched_structs.ZkWatchedMap

	listeners []serializers.DataListener
}


func loadZkHost(ctx *serializers.ThreadCtx, client curator.CuratorFramework, listener serializers.DataChangeNotifier, rootPath string) (interface{}, error) {

	node, err := watched_structs.LoadThriftWatchedNode(client, rootPath, iface.NewHostMetadata)
	if err != nil {
		return nil, err
	}

	assignments, err := watched_structs.LoadThriftWatchedNode(client, assignmentsRoot(rootPath), iface.NewHostAssignmentMetadata)
	if err != nil {
		return nil, err
	}

	state, err := watched_structs.LoadStringWatchedNode(client,
		path.Join(rootPath, STATE_PATH))

	return &ZkHost{rootPath, node, assignments, state}, nil
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

		return &ZkRing{root: root, num: iface.RingID(num), client: client, hosts: hosts}, nil
	}

	return nil, nil
}

func createZkRing(ctx *serializers.ThreadCtx, root string, num iface.RingID, listener serializers.DataChangeNotifier,  client curator.CuratorFramework) (*ZkRing, error) {
	watched_structs.CreateWithParents(client, curator.PERSISTENT, root, nil)

	hosts, err := watched_structs.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), listener, loadZkHost)
	if err != nil {
		return nil, err
	}

	return &ZkRing{root: root, num: num, client: client, hosts: hosts}, nil
}

//  public methods

func (p *ZkRing) 	AddStateChangeListener(listener serializers.DataListener) {
	p.listeners = append(p.listeners, listener)
}


func (p *ZkRing) AddHost(ctx *serializers.ThreadCtx, hostName string, port int, hostFlags []string) (iface.Host, error) {
	return CreateZkHost(ctx, p.client, p.hosts.Root, hostName, port, hostFlags)
}

func (p *ZkRing) GetHosts(ctx *serializers.ThreadCtx) []iface.Host {

	hosts := []iface.Host{}
	for _, item := range p.hosts.Values() {
		i := item.(iface.Host)
		hosts = append(hosts, i)
	}

	return hosts
}
