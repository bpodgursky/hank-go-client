package hank_iface

import "github.com/liveramp/hank/hank-core/src/main/go/hank"

type RingGroup interface {

  GetName() string

  GetRings() []Ring

  AddRing(ringNum int) Ring

  RegisterClient(metadata hank.ClientMetadata) error

  //	stub
}
