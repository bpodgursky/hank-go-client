package hank_client

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/bpodgursky/hank-go-client/fixtures"
)

func TestLock(t *testing.T) {

	sem := NewSingleLockSemaphore()

	sem.Release()
	sem.Release()

	sem.Read()

	read := false

	go func() {
		sem.Read()
		read = true
	}()

	assert.False(t, read)
	sem.Release()

	fixtures.WaitUntilOrDie(t, func()bool {
		return read
	})

}