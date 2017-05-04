package fixtures

import (
  "time"
  "errors"
  "github.com/cenkalti/backoff"
  "github.com/stretchr/testify/assert"
  "testing"
  "fmt"
  "github.com/samuel/go-zookeeper/zk"
  "github.com/curator-go/curator"
  "strconv"
)

func WaitUntilOrDie(t *testing.T, expectTrue func() bool) {

  backoffStrat := backoff.NewExponentialBackOff()
  backoffStrat.MaxElapsedTime = time.Second * 10

  err := backoff.Retry(func() error {
    val := expectTrue()

    if !val {
      return errors.New("false")
    }

    return nil

  }, backoffStrat)

  assert.Nil(t, err)

  fmt.Println("Assertion success!")

}

type logWriter struct {
  t *testing.T
  p string
}

func (lw logWriter) Write(b []byte) (int, error) {
  lw.t.Logf("%s%s", lw.p, string(b))
  return len(b), nil
}

func SetupZookeeper(t *testing.T) (*zk.TestCluster, curator.CuratorFramework) {
  cluster, _ := zk.StartTestCluster(1, nil, logWriter{t: t, p: "[ZKERR] "})
  cluster.StartAllServers()

  client := curator.NewClient("127.0.0.1:"+strconv.Itoa(cluster.Servers[0].Port), curator.NewRetryNTimes(1, time.Second))
  client.Start()

  return cluster, client
}

func TeardownZookeeper(cluster *zk.TestCluster, client curator.CuratorFramework) {
  client.Close()
  cluster.StopAllServers()
}
