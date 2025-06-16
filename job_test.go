package job

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func testDoneTask[T any](t *testing.T, task *Task[T], res T, err error) bool {
	<-task.Done()
	ok := true
	tRes := task.Res
	if !reflect.DeepEqual(res, tRes) {
		t.Errorf("expected task Res to be=%v, got=%v", res, tRes)
		ok = ok && false
	}
	tErr := task.Err
	if errors.Is(err, ErrTaskPanic) {
		if !errors.Is(tErr, ErrTaskPanic) {
			t.Errorf("expected task err to be=%v, got=%v", err, tErr)
			ok = ok && false
		}

	} else if errors.Is(err, ErrTaskCanceled) {
		if !errors.Is(tErr, ErrTaskCanceled) {
			t.Errorf("expected task err to be=%v, got=%v", err, tErr)
			ok = ok && false
		}
	} else if errors.Is(err, ErrTaskKilled) {
		if !errors.Is(tErr, ErrTaskKilled) {
			t.Errorf("expected task err to be=%v, got=%v", err, tErr)
			ok = ok && false
		}
	} else {
		if err != tErr {
			if err == nil || tErr == nil {
				t.Errorf("expected task err to be=%v, got=%v", err, tErr)
			} else if err.Error() != tErr.Error() {
				t.Errorf("expected task err to be=%v, got=%v", err, tErr)
				ok = ok && false
			}
		}
	}
	return ok
}

func TestTaskCompleteCancelAfterComplete(t *testing.T) {
	f := func() (string, error) {
		return "done", nil
	}
	task := NewTask(f)
	task.Run()
	task.Cancel()

	testDoneTask(t, task, "done", nil)

	task2 := NewTask(f)
	task2.Cancel()
	task2.Run()

	testDoneTask(t, task2, "", ErrTaskCanceled)

}

func TestTaskCompleteWithError(t *testing.T) {
	tErr := fmt.Errorf("task errored")
	f := func() (string, error) {
		return "done", tErr
	}
	task3 := NewTask(f)
	task3.Run()
	task3.Cancel()

	testDoneTask(t, task3, "done", tErr)

	task4 := NewTask(f)
	task4.Cancel()
	task4.Run()

	testDoneTask(t, task4, "", ErrTaskCanceled)
}

func TestTaskCompletesExactlyOnce(t *testing.T) {
	f := func() (string, error) {
		return "done", nil
	}
	task3 := NewTask(f)
	task3.Run()
	task3.f = func() (string, error) {
		return "done again", nil
	}
	task3.Run()

	testDoneTask(t, task3, "done", nil)
}

func TestTaskCancelWithCustomError(t *testing.T) {
	myErr := errors.New("bad")
	f := func() (string, error) {
		return "done", nil
	}
	task3 := NewTask(f)
	task3.CancelWith(myErr)

	testDoneTask(t, task3, "", myErr)
}

func TestTaskCustomComplete(t *testing.T) {
	f := func() (string, error) {
		return "done", nil
	}
	task3 := NewTask(f)
	task3.Complete("also done", nil)
	task3.Run()

	testDoneTask(t, task3, "also done", nil)
}

func TestQueueKillBeforeComplete(t *testing.T) {

	f := func() (string, error) {
		return "done", nil
	}

	ff := func() (string, error) {
		ch := make(chan string)
		<-ch
		return "done", nil
	}

	q := NewQueue[string]()
	q.Start()
	fast, _ := q.PushFunc(f)
	slow, _ := q.PushFunc(ff)
	<-fast.Done()
	q.Kill()
	<-slow.Done()
	if !testDoneTask(t, fast, "done", nil) {
		t.Error("error in fast task")
	}
	if !testDoneTask(t, slow, "", ErrTaskKilled) {
		t.Error("error in slow task")
	}
}
func TestQueueKillBeforeCompleteLoop(t *testing.T) {

	q := NewQueue[string]()

	f := func() (string, error) {
		return "done", nil
	}

	ff := func() (string, error) {
		ch := make(chan string)
		<-ch
		return "done", nil
	}

	out := q.Start()
	nTasks := 2
	fast, _ := q.PushFunc(f)
	slow, _ := q.PushFunc(ff)
	go func() {
		<-fast.Done()
		q.Kill()
	}()

	n := 0
	for range out {
		n++
	}
	if n != 2 {
		t.Errorf("expected %d tasks in out queue, got %d", nTasks, n)
	}

	if !testDoneTask(t, fast, "done", nil) {
		t.Error("error in fast task")
	}
	if !testDoneTask(t, slow, "", ErrTaskKilled) {
		t.Error("error in slow task")
	}
}
func TestQueueCustomPanic(t *testing.T) {
	q := NewQueue(
		WithPanicDefer(
			func(a any, t *string, err *error) {
				*err = fmt.Errorf("%s", a)
				*t = "zero"
			}),
	)

	f := func() (string, error) {
		panic("f panicked")
	}

	task, _ := q.PushFunc(f)
	q.Start()
	if !testDoneTask(t, task, "zero", fmt.Errorf("f panicked")) {
		t.Error("error in fast task")
	}
}

func TestQueueWrapPanic(t *testing.T) {
	q := NewQueue(
		WithPanicDefer[string](WrapPanic),
	)

	f := func() (string, error) {
		panic("f panicked")
	}

	task, _ := q.PushFunc(f)
	q.Start()
	if !testDoneTask(t, task, "", ErrTaskPanic) {
		t.Error("error in task")
	}
}

func TestQueueTasksKilledWithoutStart(t *testing.T) {
	q := NewQueue[string]()

	f := func() (string, error) {
		return "f", nil
	}

	ff := func() (string, error) {
		return "ff", nil
	}

	taskF, _ := q.PushFunc(f)
	taskFF, _ := q.PushFunc(ff)
	q.Kill()
	if !testDoneTask(t, taskF, "", ErrTaskKilled) {
		t.Error("error in task")
	}
	if !testDoneTask(t, taskFF, "", ErrTaskKilled) {
		t.Error("error in task")
	}
}

func TestQueueLoopOverOut(t *testing.T) {
	q := NewQueue[string]()

	f := func() (string, error) {
		return "f", nil
	}

	nTasks := 500
	for range nTasks {
		q.PushFunc(f)
	}
	out := q.Start()
	go func() {
		<-time.After(100 * time.Millisecond)
		q.Kill()
		for range nTasks {
			_, err := q.PushFunc(f)
			if err == nil {
				t.Errorf("expected error when pushing to killed queue, got nil error")
			}
		}
	}()
	n := 0
	for task := range out {
		n++
		testDoneTask(t, task, "f", nil)
	}

	if n != nTasks {
		t.Errorf("expected %d tasks in out queue, got %d", nTasks, n)
	}

}

func TestQueuePendingWorkSentToOutAferKill(t *testing.T) {

	q := NewQueue[string]()

	f := func() (string, error) {
		<-time.After(50 * time.Millisecond)
		return "f", nil
	}

	nTasks := 500
	for range nTasks {
		q.PushFunc(f)
	}
	q.Kill()
	out := q.Start()
	n := 0
	for tt := range out {
		n++
		testDoneTask(t, tt, "", ErrTaskKilled)
	}

	if n != nTasks {
		t.Errorf("expected %d tasks in out queue, got %d", nTasks, n)
	}
}

func TestQueuePendingWorkAndCompletedWorkSentToOutAferKill(t *testing.T) {

	q := NewQueue[string]()

	f := func() (string, error) {
		<-time.After(50 * time.Millisecond)
		return "f", nil
	}

	nTasks := 500
	for range nTasks {
		q.PushFunc(f)
	}
	out := q.Start()
	go func() {
		<-time.After(53 * time.Millisecond)
		q.Kill()
	}()
	n := 0
	errored := 0
	compl := 0
	for tt := range out {
		n++
		if tt.Err != nil {
			errored++
			testDoneTask(t, tt, "", ErrTaskKilled)
		} else {
			compl++
			testDoneTask(t, tt, "f", nil)
		}
	}

	if n != nTasks {
		t.Errorf("expected %d tasks in out queue, got %d", nTasks, n)
	}
	fmt.Printf("total=%d\n", n)
	fmt.Printf("compl=%d\n", compl)
	fmt.Printf("errored=%d\n", errored)
	if compl == 0 {
		t.Errorf("expected at least one task to finish, got %d", compl)
	}
}
