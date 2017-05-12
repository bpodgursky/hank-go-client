package watched_structs

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/curator-go/curator"
	"path"
	"path/filepath"
)

func AssertEmpty(client curator.CuratorFramework, fullPath string) error {
	exists, _ := client.CheckExists().ForPath(fullPath)
	if exists != nil {
		return errors.New("Domain group already exists!")
	}
	return nil
}

func AssertExists(client curator.CuratorFramework, fullPath string) error {
	exists, _ := client.CheckExists().ForPath(fullPath)
	if exists == nil {
		return errors.New("Domain group doesn't exist!")
	}
	return nil
}

func CreateWithParents(client curator.CuratorFramework, mode curator.CreateMode, root string, data []byte) error {
	builder := client.Create().WithMode(mode).CreatingParentsIfNeeded()

	if data != nil {
		_, createErr := builder.ForPathWithData(root, data)
		return createErr
	} else {
		_, createErr := builder.ForPath(root)
		return createErr
	}

}

func SafeEnsureParents(client curator.CuratorFramework, mode curator.CreateMode, root string) error {

	parentExists, existsErr := client.CheckExists().ForPath(root)
	if existsErr != nil {
		return existsErr
	}

	if parentExists == nil {
		return CreateWithParents(client, mode, root, nil)
	}

	return nil
}

func LoadThrift(ctx *serializers.ThreadCtx, path string, client curator.CuratorFramework, tStruct thrift.TStruct) error {
	data, err := client.GetData().ForPath(path)
	if err != nil {
		return err
	}

	readErr := ctx.ReadThriftBytes(data, tStruct)
	if readErr != nil {
		return readErr
	}

	return nil
}

func CreateEphemeralSequential(root string, framework curator.CuratorFramework) serializers.SetBytes {
	return func(data []byte) error {
		_, err := framework.Create().WithMode(curator.EPHEMERAL_SEQUENTIAL).ForPathWithData(root, data)
		return err
	}
}

func IsSubdirectory(root string, otherPath string) bool {

	cleanRoot := path.Clean(root)
	cleanRel := path.Clean(otherPath)

	if cleanRoot == cleanRel {
		return false
	}

	rel, _ := filepath.Rel(root, otherPath)
	return path.Join(cleanRoot, rel) == cleanRel

}
