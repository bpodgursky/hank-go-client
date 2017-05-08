package hank_zk

import (
  "errors"
  "github.com/curator-go/curator"
  "hank-go-client/hank_thrift"
  "git.apache.org/thrift.git/lib/go/thrift"
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

func LoadThrift(ctx *hank_thrift.ThreadCtx, path string, client curator.CuratorFramework, tStruct thrift.TStruct) error {
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

func CreateEphemeralSequential(root string, framework curator.CuratorFramework) hank_thrift.SetBytes {
  return func(data []byte) error {
    _, err := framework.Create().WithMode(curator.EPHEMERAL_SEQUENTIAL).ForPathWithData(root, data)
    return err
  }
}
