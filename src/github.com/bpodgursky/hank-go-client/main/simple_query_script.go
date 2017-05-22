package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/hank_client"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/curator-go/curator"
	"os"
	"strings"
	"time"
)

func main() {
	argsWithoutProg := os.Args[1:]

	client := curator.NewClient(argsWithoutProg[0], curator.NewRetryNTimes(1, time.Second))
	client.Start()

	ctx := serializers.NewThreadCtx()

	coordinator, coordErr := coordinator.NewZkCoordinator(client, "/hank/domains", "/hank/ring_groups", "/hank/domain_groups")
	if coordErr != nil {

		fmt.Println(coordErr)
		return
	}

	group := coordinator.GetRingGroup("spruce-aws")
	ring0 := group.GetRing(iface.RingID(0))

	hosts := ring0.GetHosts(ctx)
	host := hosts[0]

	conn, err := hank_client.NewHostConnection(host, 100, 100, 100, 100)

	if err != nil {
		fmt.Println(err)
		return
	}

	dg := coordinator.GetDomainGroup("spruce-aws")

	versions := dg.GetDomainVersions(ctx)
	domainVersion := versions[0]

	reader := bufio.NewReader(os.Stdin)

	for {

		fmt.Println("Enter hex arl: ")
		text, _ := reader.ReadString('\n')
		fmt.Println(text)

		bytes, err := hex.DecodeString(strings.TrimSpace(text))
		if err != nil {
			fmt.Println(err)
			return
		}

		val, err := conn.Get(domainVersion.DomainID, bytes)
		if err != nil {
			fmt.Println(err)
			return
		}

		if val.Value != nil {
			fmt.Println("Found value")
			encodeToString := hex.EncodeToString(val.Value)
			fmt.Println("Value: ", encodeToString)
		} else {
			fmt.Println("Did not find value")
		}

	}

}
