package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type Args struct{}

// TODO: should be broken down into map and reduce structs?
type Task struct {
	ID        int
	TaskType  TaskType
	StartTime int64 // FIXME: passing `nil` through json from rpc?

	MapFile     string
	MapContents string

	ReduceKey string
	// todo: will be populated on task call. is this ok?
	// ReduceValues []string

	NReduce int
}

func (t *Task) IsZero() bool {
	return t.ID == 0 &&
		t.TaskType == 0 &&
		t.StartTime == 0 &&
		t.MapFile == "" &&
		t.MapContents == "" &&
		t.ReduceKey == "" &&
		// t.ReduceValues == nil &&
		t.NReduce == 0
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
