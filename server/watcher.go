package server

import (
	"fmt"
	"github.com/pkg/errors"
	"time"
)

type Key string

func NewWatcher(size int, logger Logger) *Watcher {
	return &Watcher{
		logger:    logger,
		ch:        make(chan Key, size),
		functions: make(map[Key][]func()),
	}
}

type Watcher struct {
	logger    Logger
	ch        chan Key
	functions map[Key][]func()
	closeCH   chan struct{}
}

func (b *Watcher) OnNotify(change Key, fn func()) {
	b.functions[change] = append(b.functions[change], fn)
}

func (b *Watcher) Notify(change Key) {
	b.logger.Info("notify.", "change", string(change))
	b.ch <- change
	b.logger.Info("notify: ok.", "change", string(change))
}

func (b *Watcher) Start() {
	b.logger.Info("start watcher.")
	for {
		select {
		case <-b.closeCH:
			b.logger.Info("watcher exit.")
			return
		default:
			change := make(map[Key]struct{})
			func() {
				for {
					select {
					case n := <-b.ch:
						change[n] = struct{}{}
					case <-time.After(time.Second * 2):
						return
					}
				}
			}()

			for t := range change {
				for _, fn := range b.functions[t] {
					go func() {
						defer func() {
							if err := recover(); err != nil {
								b.logger.Error("recover from panic.", "error", errors.WithStack(errors.New(fmt.Sprint(err))))
							}
						}()
						fn()
					}()
				}
			}
		}
	}
}

func (b *Watcher) Close() {
	close(b.closeCH)
}
