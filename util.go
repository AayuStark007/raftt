package main

import "time"

func CreateTimeoutCallback(timeoutMs int, callbackFn func()) (cancelFn func()) {
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	signal := make(chan bool, 1)

	cancelFn = func() {
		if timer.Stop() {
			signal <- true
		}
	}

	go func() {
		select {
		case <-timer.C:
			callbackFn()
		case <-signal:
		}
	}()

	return
}
