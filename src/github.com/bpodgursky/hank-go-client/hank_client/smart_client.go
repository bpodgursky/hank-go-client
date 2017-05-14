package hank_client

import (
  "github.com/liveramp/hank/hank-core/src/main/go/hank"
  "os"
  "time"
  "github.com/bpodgursky/hank-go-client/serializers"
  "github.com/bpodgursky/hank-go-client/iface"
  "fmt"
  "github.com/bpodgursky/hank-go-client/thrift_services"
  "errors"
  "strconv"
)

type HankSmartClient struct {
  coordinator iface.Coordinator
  ringGroup   iface.RingGroup
}

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

  return &HankSmartClient{coordinator, ringGroup}, nil
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

func (p *HankSmartClient) updateConnectionCache(ctx *serializers.ThreadCtx) {
  fmt.Println("Loading Hank's smart client metadata cache and connections.")

  newServerToConnections := make(map[*iface.HostAddress]*thrift_services.HostConnectionPool)

  newDomainToPartitionToConnections := make(map[iface.DomainID]map[iface.PartitionID]*thrift_services.HostConnectionPool)

  p.buildNewConnectionCache(ctx, &newServerToConnections, &newDomainToPartitionToConnections)

}

func (p*HankSmartClient) isPreferredHost(host iface.Host) bool {

  fmt.Println("Environment flags for host ", host)

  //  TODO
  return false
}

func (p*HankSmartClient) buildNewConnectionCache(
  ctx *serializers.ThreadCtx,
  newServerToConnections *map[*iface.HostAddress]*thrift_services.HostConnectionPool,
  newDomainToPartitionToConnections *map[iface.DomainID]map[iface.PartitionID]*thrift_services.HostConnectionPool) error {

  //  this is horrible looking, and I'd love to use MultiMap, but I can't  because this horseshit,
  //  gimp, special-ed language thinks that generics are too dangerous and just gives you fucking crayons
  domainToPartToAddresses := make(map[iface.DomainID]map[iface.PartitionID][]*iface.HostAddress)

  preferredHosts := []iface.Host{}

  for _, ring := range p.ringGroup.GetRings() {
    fmt.Println("Building connection cache for ", ring)

    for _, host := range ring.GetHosts(ctx) {
      fmt.Println("Building cache for host: ", host)

      if p.isPreferredHost(host) {
        preferredHosts = append(preferredHosts, host)
      }

      address := host.GetAddress(ctx)
      fmt.Println("Loading partition metadata for Host: ", address)

      hostAddress := &iface.HostAddress{Ring: ring, Address: address}

      for _, hostDomain := range host.GetAssignedDomains(ctx) {

        domain, err := hostDomain.GetDomain(ctx, p.coordinator)
        if err != nil {
          return err
        }

        domainId := domain.GetId(ctx)

        if domain == nil {
          return errors.New("Domain not found "+strconv.Itoa(int(domainId)))
        }

        partitionToAddresses := domainToPartToAddresses[domainId]

        if partitionToAddresses == nil {
          domainToPartToAddresses[domainId] = make(map[iface.PartitionID][]*iface.HostAddress)
        }


        fmt.Println(hostAddress)

        //for (HostDomainPartition partition : hostDomain.getPartitions()) {
        //if (!partition.isDeletable()) {
        //List<HostAddress> partitionsList = partitionToAdresses.get(partition.getPartitionNumber());
        //if (partitionsList == null) {
        //partitionsList = new ArrayList<HostAddress>();
        //partitionToAdresses.put(partition.getPartitionNumber(), partitionsList);
        //}
        //partitionsList.add(hostAddress);
        //}
        //}


      }
    }

  }

  return nil
}
