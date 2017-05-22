package hank_client

type hankSmartClientOptions struct {
	NumConnectionsPerHost int32

	TryLockTimeoutMs             int32
	EstablishConnectionTimeoutMs int32
	QueryTimeoutMs               int32
	BulkQueryTimeoutMs           int32
}


func NewHankSmartClientOptions() *hankSmartClientOptions {
	return &hankSmartClientOptions{
		NumConnectionsPerHost:        int32(1),
		TryLockTimeoutMs:             int32(1000),
		EstablishConnectionTimeoutMs: int32(1000),
		QueryTimeoutMs:               int32(1000),
		BulkQueryTimeoutMs:           int32(1000),
	}
}

func (p *hankSmartClientOptions) SetNumConnectionsPerHost(connections int32) *hankSmartClientOptions {
	p.NumConnectionsPerHost = connections
	return p
}

func (p *hankSmartClientOptions) SetTryLockTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.TryLockTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetEstablishConnectionTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.EstablishConnectionTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetQueryTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.QueryTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetBulkQueryTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.BulkQueryTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) Build() *hankSmartClientOptions {
	build := &hankSmartClientOptions{}

	return build
}
