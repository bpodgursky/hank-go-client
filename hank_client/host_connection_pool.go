package hank_client

import "github.com/bpodgursky/hank-go-client/serializers"

type HostConnectionPool struct {
}

func CreateHostConnectionPool(ctx *serializers.ThreadCtx, connections []*HostConnection, hostShuffleSeed int32, preferredHosts []string) *HostConnectionPool {

	asMap := make(map[string][]*HostConnection)

	for _, connection := range connections {
		address := connection.host.GetAddress(ctx).Print()

		connections := asMap[address]
		if connections == nil {
			connections = []*HostConnection{}
		}

		asMap[address] = append(connections, connection)

	}

	return NewHostConnectionPool(asMap, hostShuffleSeed, preferredHosts)

}

func NewHostConnectionPool(connectionsByHost map[string][]*HostConnection, hostShuffleSeed int32, preferredHosts []string) *HostConnectionPool{

	//	TODO
return nil



}
