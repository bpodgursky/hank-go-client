package test

import (
	"time"
	"errors"
	"github.com/cenkalti/backoff"
	"github.com/stretchr/testify/assert"
	"testing"
)

func WaitUntilOrDie(t *testing.T, expectTrue func() bool) {

	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 15

	err := backoff.Retry(func() error{
		val := expectTrue()

		if !val {
			return errors.New("false")
		}

		return nil

	}, backoffStrat)

	assert.Nil(t, err)

}