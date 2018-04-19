package huton

import (
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func TestPeriodicCompactor(t *testing.T) {
	interval := time.Second
	compactor := PeriodicCompactor(interval)
	instance := &Instance{logger: log.New(ioutil.Discard, "", 0)}
	cache := newCache("testCache", instance)
	errCh := make(chan error, 1)
	shutdownCh := make(chan struct{})
	go compactor(cache, errCh, shutdownCh)
	time.Sleep(2 * time.Second)
	close(shutdownCh)
	close(errCh)
	if len(errCh) != 0 {
		t.Errorf("Err channel containers errors.")
		for err := range errCh {
			t.Errorf("    -> %s", err)
		}
	}
}
