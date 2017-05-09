package coordinator

import (
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
	"github.com/bpodgursky/hank-go-client/serializers"
	"github.com/bpodgursky/hank-go-client/iface"
)

//  suuuuure
type Getter func(name string) interface{}

//  this file is the horrifying result of not having generics, as far as I can tell.  is there any way I can avoid
//  declaring this for every single thrift type I want?  maybe we can template and autogenerate it?

// watched node cast copypasta

func GetDomainGroupMetadata(ctx *serializers.ThreadCtx, get serializers.GetBytes) (*hank.DomainGroupMetadata, error) {
	metadata := hank.NewDomainGroupMetadata()
	error := ctx.ReadThrift(get, metadata)
	if error != nil {
		return nil, error
	}
	return metadata, nil
}

func GetHostMetadata(ctx *serializers.ThreadCtx, get serializers.GetBytes) (*hank.HostMetadata, error) {
	metadata := hank.NewHostMetadata()
	error := ctx.ReadThrift(get, metadata)
	if error != nil {
		return nil, error
	}
	return metadata, nil
}

//  watched thrift map cast copypasta

func GetDomainGroup(name string, get Getter) iface.DomainGroup {
	raw := get(name)
	original, ok := raw.(iface.DomainGroup)
	if ok {
		return original
	}
	return nil
}

func GetRingGroup(name string, get Getter) iface.RingGroup {
	raw := get(name)
	original, ok := raw.(iface.RingGroup)
	if ok {
		return original
	}
	return nil
}

func GetClientMetadata(name string, get Getter) *hank.ClientMetadata {
	raw := get(name)
	original, ok := raw.(hank.ClientMetadata)
	if ok {
		return &original
	}
	return nil
}

func GetRing(name string, get Getter) iface.Ring {
	raw := get(name)
	original, ok := raw.(iface.Ring)
	if ok {
		return original
	}
	return nil
}
