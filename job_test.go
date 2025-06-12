package job

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func testDoneTask[T any](t *testing.T, task *Task[T], res T, err error) bool {
	<-task.Done()
	tRes := task.Res
	if !reflect.DeepEqual(res, tRes) {
		t.Errorf("expected task Res to be=%v, got=%v", res, tRes)
		return false
	}
	tErr := task.Err
	if err != tErr {
		t.Errorf("expected task err to be=%v, got=%v", err, tErr)
		return false
	}
	return true
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

func TestQueue(t *testing.T) {

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
	fast := q.PushFunc(f)
	slow := q.PushFunc(ff)
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
