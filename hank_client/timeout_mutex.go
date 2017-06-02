package hank_client

import (
	"time"
)

type TimeoutMutex struct {
	c chan struct{}
}

func NewMutex() *TimeoutMutex {
	return &TimeoutMutex{make(chan struct{}, 1)}
}

func (m *TimeoutMutex) Lock() {
	m.c <- struct{}{}
}

func (m *TimeoutMutex) Unlock() {
	<-m.c
}

func (m *TimeoutMutex) TryLock(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	select {
	case m.c <- struct{}{}:
		timer.Stop()
		return true
	case <-time.After(timeout):
		return false
	}
}

func (m *TimeoutMutex) TryLockNoWait() bool {
	return m.TryLock(0)
}
