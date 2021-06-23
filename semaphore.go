package producer

// channel based semaphore
// used to limit the number of concurrent goroutines
type semaphore chan struct{}

// acquire a lock, blocking or non-blocking
func (s semaphore) acquire() {
	s <- struct{}{}
}

// release a lock
func (s semaphore) release() {
	<-s
}

// wait block until the last goroutine release the lock
func (s semaphore) wait(count int) {
	for i := 0; i < count; i++ {
		s <- struct{}{}
	}
}

// releases the semaphore for use again after a wait call
// only use this after calling wait()
func (s semaphore) open(count int) {
	for i := 0; i < count; i++ {
		<-s
	}
}
