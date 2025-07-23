package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	MapTask    TaskType = iota // 0
	ReduceTask                 // 1
)

type Task struct {
	TaskType string // "Map" or "Reduce"
	TaskID   int    // Map task number or Reduce task number
	Filename string // For Map tasks, the name of the file to process
	ReduceID int    // For Map tasks, the number of reduce tasks
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type AssignTaskArgs struct {
	WorkerID int // Unique ID for the worker
}

type AssignTaskReply struct {
	Task Task // The task assigned to the worker
}

type TaskCompleteArgs struct {
	TaskID int // The ID of the completed task
}

type TaskCompleteReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
