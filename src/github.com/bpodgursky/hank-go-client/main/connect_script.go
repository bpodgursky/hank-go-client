package main

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/coordinator"
	"github.com/bpodgursky/hank-go-client/hank_client"
	"github.com/curator-go/curator"
	"os"
	"time"
)

func main() {
	argsWithoutProg := os.Args[1:]

	client := curator.NewClient(argsWithoutProg[0], curator.NewRetryNTimes(1, time.Second))

	startErr := client.Start()
	if startErr != nil {
		fmt.Println(startErr)
		return
	}

	coordinator, coordErr := coordinator.NewZkCoordinator(client, "/hank/ring_groups", "/hank/domain_groups")
	if coordErr != nil {
		fmt.Println(startErr)
		return
	}

	smartClient, clientErr := hank_client.NewHankSmartClient(coordinator, "spruce-aws")
	if clientErr != nil {
		fmt.Println(clientErr)
		return
	}

	fmt.Println(smartClient)

	time.Sleep(time.Hour)

}
