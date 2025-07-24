package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Stage int

const (
	MapReady    Stage = iota // 0
	ReduceReady              // 1
	AllDone                  // 2
)

type Coordinator struct {
	files      []string
	nReduce    int
	stage      Stage
	taskQueue  chan Task
	taskActive map[int]chan struct{}
	taskId     atomic.Int64
	wg         sync.WaitGroup
	mu         sync.Mutex // Add mutex to protect taskActive map
}

// Assign map and reduce tasks to workers
// This function is called by the worker to get a task.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	if c.stage != AllDone {
		task := <-c.taskQueue
		reply.Task = task

		c.mu.Lock()
		c.taskActive[task.TaskID] = make(chan struct{})
		taskActive := c.taskActive[task.TaskID]
		c.mu.Unlock()

		go func(taskActive chan struct{}) {
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()

			// Wait for the worker to complete the task or timeout
			select {
			case <-taskActive:
				return
			case <-timer.C:
				c.taskQueue <- task
			}
		}(taskActive)
	} else {
		c.Done()
	}
	return nil

}

// Mark a task as complete.
// The function signals that the task is complete and decrements the wait group counter.
// If the task is not found or already completed, it returns an error.
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	taskID := args.TaskID

	c.mu.Lock()
	taskActive := c.taskActive[taskID]
	if taskActive != nil {
		delete(c.taskActive, taskID) // Remove completed task
	}
	c.mu.Unlock()

	if taskActive != nil {
		taskActive <- struct{}{} // Signal that the task is complete
		close(taskActive)
		c.wg.Done() // Decrement the wait group counter
		return nil
	} else {
		return errors.New("task not found or already completed")
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := (c.stage == AllDone)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		stage:      MapReady,
		taskId:     atomic.Int64{},
		taskQueue:  make(chan Task, nReduce),
		taskActive: make(map[int]chan struct{}),
		wg:         sync.WaitGroup{},
	}
	c.wg.Add(len(files))

	// Initialise map tasks for each file
	go func(files []string, nReduce int) {
		for _, file := range files {
			mapTask := Task{
				TaskType: MapTask,
				TaskID:   int(c.taskId.Load()),
				Filename: file,
				ReduceID: nReduce,
			}
			c.taskId.Add(1)
			c.taskQueue <- mapTask
		}
		c.wg.Wait()
		c.stage = ReduceReady
		c.wg.Add(nReduce)

		// Initialise reduce tasks
		for i := range nReduce {
			reduceTask := Task{
				TaskType: ReduceTask,
				TaskID:   int(c.taskId.Load()),
				Filename: "",
				ReduceID: i,
			}
			c.taskId.Add(1)
			c.taskQueue <- reduceTask
		}

		c.wg.Wait()
		c.stage = AllDone

		// Close the task queue and active channels
		close(c.taskQueue)
		for _, taskActive := range c.taskActive {
			close(taskActive)
		}
	}(files, nReduce)

	c.server()
	return &c
}
