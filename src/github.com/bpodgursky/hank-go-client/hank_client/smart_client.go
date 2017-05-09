package hank_client

import (
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"os"
	"time"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/iface"
)

type HankSmartClient struct{}

func NewHankSmartClient(
	coordinator iface.Coordinator,
	ringGroupName string) (*HankSmartClient, error) {

	ringGroup := coordinator.GetRingGroup(ringGroupName)

	metadata, err := GetClientMetadata()

	if err != nil {
		return nil, err
	}

	ctx := serializers.NewThreadCtx()
	registerErr := ringGroup.RegisterClient(ctx, metadata)

	if registerErr != nil {
		return nil, registerErr
	}

	return &HankSmartClient{}, nil
}

func GetClientMetadata() (*hank.ClientMetadata, error) {

	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	metadata := hank.NewClientMetadata()
	metadata.Host = hostname
	metadata.ConnectedAt = time.Now().Unix() * int64(1000)
	metadata.Type = "GolangHankSmartClient"
	metadata.Version = "lolidk"

	return metadata, nil
}
