package coordinator

import (
  "errors"
  "github.com/curator-go/curator"
)

func assertEmpty(client curator.CuratorFramework, fullPath string) error{
  exists, _ := client.CheckExists().ForPath(fullPath)
  if exists != nil {
    return errors.New("Domain group already exists!")
  }
  return nil
}


func assertExists(client curator.CuratorFramework, fullPath string) error {
  exists, _ := client.CheckExists().ForPath(fullPath)
  if exists == nil {
    return errors.New("Domain group doesn't exist!")
  }
  return nil
}