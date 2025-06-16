package job

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrTaskKilled   error = errors.New("Task killed")
	ErrTaskCanceled error = errors.New("Task canceled")
)

type Task[T any] struct {
	f    func() (T, error)
	Res  T
	Err  error
	once sync.Once
	mu   sync.Mutex
	done chan struct{}
}

func NewTask[T any](f func() (T, error)) *Task[T] {
	return &Task[T]{
		f:    f,
		done: make(chan struct{}),
	}
}

func (t *Task[T]) Done() <-chan struct{} {
	return t.done
}

func (t *Task[T]) Complete(result T, err error) {
	t.complete(result, err)
}

func (t *Task[T]) Run() {
	res, err := t.f()
	t.Complete(res, err)
}

func (t *Task[T]) Cancel() {
	var zero T
	t.complete(zero, ErrTaskCanceled)
}

func (t *Task[T]) CancelWith(err error) {
	var zero T
	t.complete(zero, err)
}

func (t *Task[T]) complete(result T, err error) {
	t.once.Do(func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.Res = result
		t.Err = err
		close(t.done)
	})
}

type opts[T any] struct {
	panicDefer func(any, *T, *error)
	numWorkers int
	queueBuf   int
}

type option[T any] func(*opts[T])

func WithWorkers[T any](n int) option[T] {
	return func(o *opts[T]) {
		o.numWorkers = n
	}
}

func WithQueueBuffer[T any](n int) option[T] {
	return func(o *opts[T]) {
		o.queueBuf = n
	}
}

func WithPanicDefer[T any](f func(any, *T, *error)) option[T] {
	return func(o *opts[T]) {
		o.panicDefer = f
	}
}

func defaultOpts[T any]() *opts[T] {
	return &opts[T]{
		nil,
		1,
		0,
	}
}

type TaskQueue[T any] struct {
	work    chan *Task[T]
	bouncer chan *Task[T]
	done    chan struct{}
	wg      sync.WaitGroup
	opts    *opts[T]
}

// TODO: return true if task was pushed successfully
func (tq *TaskQueue[T]) PushFunc(f func() (T, error)) *Task[T] {
	var res T
	ch := make(chan struct{})
	t := &Task[T]{
		f,
		res,
		nil,
		sync.Once{},
		sync.Mutex{},
		ch,
	}
	tq.tryEnqueue(t)
	return t
}

func (tq *TaskQueue[T]) Push(t *Task[T]) {
	tq.tryEnqueue(t)
}

func (tq *TaskQueue[T]) tryEnqueue(t *Task[T]) {
	go func() {
		select {
		case tq.bouncer <- t:
		case <-tq.done:
			t.CancelWith(ErrTaskKilled)
		}
	}()
}

func (tq *TaskQueue[T]) Kill() int {
	close(tq.done)
	tq.wg.Wait()

	return len(tq.work)
}

func NewQueue[T any](options ...option[T]) *TaskQueue[T] {
	opts := defaultOpts[T]()
	for _, o := range options {
		o(opts)
	}
	work := make(chan *Task[T], opts.queueBuf)
	bouncer := make(chan *Task[T])
	q := &TaskQueue[T]{
		work:    work,
		done:    make(chan struct{}),
		opts:    opts,
		bouncer: bouncer,
	}

	return q
}

// TODO: init bouncer here? pushes to queue before start should error?
func (tq *TaskQueue[T]) Start() chan *Task[T] {

	out := make(chan *Task[T])
	compl := BusyChan[*Task[T]]{ch: make(chan *Task[T])}
	tq.wg.Add(tq.opts.numWorkers)
	for range tq.opts.numWorkers {
		go func() {
			defer tq.wg.Done()
			tq.runWorker(tq.work, out)
		}()
	}
	tq.wg.Add(1)
	go func() {
		defer tq.wg.Done()
		for {
			select {
			case guest := <-tq.bouncer:
				tq.work <- guest
			case <-tq.done:
				close(tq.work)
				fmt.Println("work closed")
				tq.cancelWork(tq.work, out)
				close(out)
				return
			}
		}
	}()

	go func() {
		for t := range out {
			compl.Send(t)
		}
		compl.Close()
	}()

	return compl.Ch()
}

func (tq *TaskQueue[T]) runWorker(work <-chan *Task[T], out chan<- *Task[T]) {
	for t := range work {
		go func() {
			tt := tq.runTask(t)
			select {
			case <-tq.done:
			case out <- tt:
			}
		}()
	}
}

func (tq *TaskQueue[T]) cancelWork(ch chan *Task[T], out chan *Task[T]) {
	for t := range ch {
		select {
		case <-t.Done():
		default:
			t.CancelWith(ErrTaskKilled)
		}
		out <- t
	}
}

type taskRes[T any] struct {
	res T
	err error
}

func (tq *TaskQueue[T]) runTask(t *Task[T]) *Task[T] {
	resCh := make(chan taskRes[T], 1)
	select {
	case <-t.Done():
		return t
	case <-tq.done:
		t.CancelWith(ErrTaskKilled)
		return t
	default:
		go func() {
			var err error
			var res T
			res, err = tq.wrapWithRecover(t.f)
			resCh <- taskRes[T]{res, err}
		}()
		select {
		case <-tq.done:
			t.CancelWith(ErrTaskKilled)
			return t
		case res := <-resCh:
			t.complete(res.res, res.err)
			return t
		}
	}
}

var ErrTaskPanic error = errors.New("Task panic")

func WrapPanic[T any](rec any, _ *T, err *error) {
	*err = fmt.Errorf("%v: %w", rec, ErrTaskPanic)
}

func (tq *TaskQueue[T]) wrapWithRecover(f func() (T, error)) (res T, err error) {

	if tq.opts.panicDefer != nil {
		defer func() {
			if r := recover(); r != nil {
				tq.opts.panicDefer(r, &res, &err)
			}
		}()
	}
	res, err = f()
	return
}

type BusyChan[T any] struct {
	ch chan T
	wg sync.WaitGroup
}

func (bc *BusyChan[T]) Send(val T) {
	bc.wg.Add(1)
	go func() {
		defer bc.wg.Done()
		bc.ch <- val
	}()
}

func (bc *BusyChan[T]) Ch() chan T {
	return bc.ch
}

func (bc *BusyChan[T]) Close() {
	bc.wg.Wait()
	close(bc.ch)
}
