package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// Execute a Map task.
// This function reads the input file, applies the map function,
// and writes the intermediate KeyValue pairs to temporary files.
// The intermediate files are named "mr-<taskID>-<reduceID>".
func execMapTask(mapf func(string, string) []KeyValue, taskID int, filename string, nReduce int) {
	// Open the file and read its content.
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Map Task: cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Map Task: cannot read %v", filename)
	}
	file.Close()

	// Get the intermediate KeyValue pairs from the map function.
	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		intermediate[reduceID] = append(intermediate[reduceID], kv)
	}

	// Write the intermediate KeyValue pairs to temporary files in JSON.
	for i := range nReduce {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		tempFIle, err := os.CreateTemp("./", intermediateFileName)
		if err != nil {
			log.Fatalf("Map Task: cannot create temp file %v", intermediateFileName)
		}

		encoder := json.NewEncoder(tempFIle)
		for _, kv := range intermediate[i] {
			if err := encoder.Encode(&kv); err != nil {
				log.Fatalf("Map Task: cannot encode KeyValue %v", kv)
			}
		}

		// Rename the temp file to the final intermediate file name.
		tempFIle.Close()
		err = os.Rename(tempFIle.Name(), intermediateFileName)
		if err != nil {
			log.Fatalf("Map Task: cannot rename temp file %v to %v", tempFIle.Name(), intermediateFileName)
		}
	}

	// Notify the coordinator that the task is complete.
	CallTaskComplete(taskID)
}


// Controller functions for RPC AssignTask
func CallAssignTask() (*AssignTaskReply, error) {
	args := AssignTaskArgs{} // Example worker ID
	reply := AssignTaskReply{}
	call("Coordinator.AssignTask", &args, &reply)
	return &reply, nil
}

// Controller functions for RPC TaskComplete
func CallTaskComplete(taskID int) {
	args := TaskCompleteArgs{TaskID: taskID}
	reply := TaskCompleteReply{}
	call("Coordinator.TaskComplete", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
