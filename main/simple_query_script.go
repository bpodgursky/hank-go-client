package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/curator-go/curator"
	"log"
	"os"
	"strings"
	"time"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/hank_client"
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

	domain := coordinator.GetDomain(argsWithoutProg[1])
	domainId := domain.GetId(ctx)

	fmt.Println("Using domain: ", domain.GetName())

	file, err := os.Open(argsWithoutProg[2])
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		bytes := scanner.Bytes()
		text := string(bytes)

		fmt.Println("Checking: ", text)

		bytes, err := hex.DecodeString(strings.TrimSpace(text))
		if err != nil {
			fmt.Println(err)
			return
		}

		//TODO get arl type
		//
		//ctx.ReadThriftBytes(bytes, )

		val, err := conn.Get(domainId, bytes, false)
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
