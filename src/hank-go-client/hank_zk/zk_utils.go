package hank_zk

import (
  "errors"
  "github.com/curator-go/curator"
)

func AssertEmpty(client curator.CuratorFramework, fullPath string) error{
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

func SafeEnsureParents(client curator.CuratorFramework, root string) error{

  parentExists, existsErr := client.CheckExists().ForPath(root)
  if existsErr != nil{
    return existsErr
  }

  if parentExists == nil {
    _, createErr := client.Create().CreatingParentsIfNeeded().ForPath(root)
    if createErr != nil{
      return createErr
    }
  }

  return nil
}


