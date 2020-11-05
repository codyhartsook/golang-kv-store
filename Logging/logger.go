// Package alog provides a simple asynchronous logger that will write to provided io.Writers without blocking calling
// goroutines.
package alog

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// AsyncLog is a type that defines a logger. It can be used to write log messages synchronously (via the Write method)
// or asynchronously via the channel returned by the MessageChannel accessor.
type AsyncLog struct {
	dest               io.Writer
	m                  *sync.Mutex
	msgCh              chan string
	errorCh            chan error
	shutdownCh         chan struct{}
	shutdownCompleteCh chan struct{}
}

// New creates a new AsyncLog object that writes to the provided io.Writer.
// If nil is provided the output will be directed to os.Stdout.
func New(w io.Writer) *AsyncLog {
	// Initialize fields
	alog := new(AsyncLog)
	alog.msgCh = make(chan string)
	alog.errorCh = make(chan error)

	alog.shutdownCh = make(chan struct{})
	alog.shutdownCompleteCh = make(chan struct{})
	alog.m = &sync.Mutex{}

	if w == nil {
		w = os.Stdout
	}

	alog.dest = w
	return alog
}

// Start begins the message loop for the asynchronous logger. It should be initiated as a goroutine to prevent
// the caller from being blocked.
func (al AsyncLog) Start() {
	var msg string
	wg := &sync.WaitGroup{}
loop:

	for {
		select {
		case msg = <-al.msgCh:
			wg.Add(1)
			go al.write(msg, wg)
		case <-al.shutdownCh:
			wg.Wait()
			al.shutdown()
			break loop
		default:
			continue
		}
	}
}

func (al AsyncLog) formatMessage(msg string) string {
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	return fmt.Sprintf("[%v] - %v", time.Now().Format("2006-01-02 15:04:05"), msg)
}

func (al AsyncLog) write(msg string, wg *sync.WaitGroup) {

	al.m.Lock()
	defer al.m.Unlock()
	defer wg.Done()
	_, err := al.dest.Write([]byte(al.formatMessage(msg)))

	// write to error chan if necesary
	if err != nil {
		go func(err error) {
			al.errorCh <- err
		}(err)
	}
}

func (al AsyncLog) shutdown() {

	// close the message channel
	close(al.msgCh)
	al.shutdownCompleteCh <- struct{}{}

}

// MessageChannel returns a channel that accepts messages that should be written to the log.
func (al AsyncLog) MessageChannel() chan<- string {
	return al.msgCh
}

// ErrorChannel returns a channel that will be populated when an error is raised during a write operation.
// This channel should always be monitored in some way to prevent deadlock goroutines from being generated
// when errors occur.
func (al AsyncLog) ErrorChannel() <-chan error {
	return al.errorCh
}

// Stop shuts down the logger. It will wait for all pending messages to be written and then return.
// The logger will no longer function after this method has been called.
func (al AsyncLog) Stop() {
	al.shutdownCh <- struct{}{}
	<-al.shutdownCompleteCh
}

// Write synchronously sends the message to the log output
func (al AsyncLog) Write(msg string) (int, error) {
	return al.dest.Write([]byte(al.formatMessage(msg)))
}
