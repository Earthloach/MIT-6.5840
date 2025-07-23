package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Processing tasks until the coordinator stops.
	for {
		taskRes, err := CallAssignTask()
		if err != nil {
			log.Fatalf("Worker: failed to assign task: %v", err)
		}
		switch taskRes.Task.TaskType {
		case MapTask:
			execMapTask(mapf, taskRes.Task.TaskID, taskRes.Task.Filename, taskRes.Task.ReduceID)
		case ReduceTask:
			execReduceTask(reducef, taskRes.Task.TaskID, taskRes.Task.ReduceID)
		default:
			fmt.Println("Worker: finished all tasks")
			return
		}
	}
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
		intermediateFileName := fmt.Sprintf("mr-%v-%v", taskID, i)
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

// Execute a Reduce task.
// This function reads all intermediate files for the given reduce task,
// applies the reduce function, and writes the output to a file named "mr-out-<reduceID>".
func execReduceTask(reducef func(string, []string) string, taskID int, reduceID int) {
	files, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("Reduce Task: cannot read directory: %v", err)
	}

	kva := []KeyValue{}

	// Read all intermediate files for this reduce task.
	for _, file := range files {

		// Check if the file is an intermediate file for this reduce task
		if file.IsDir() || !strings.HasPrefix(file.Name(), "mr-") ||
			!strings.HasSuffix(file.Name(), fmt.Sprintf("-%d", reduceID)) {
			continue
		}

		f, err := os.Open(file.Name())
		if err != nil {
			log.Fatalf("Reduce Task: cannot open %v", file.Name())
		}

		decoder := json.NewDecoder(f)

		// Decode the KeyValue pairs from the file.
		var kv KeyValue
		for decoder.Decode(&kv) == nil {
			kva = append(kva, kv)
		}
		f.Close()
	}

	fileName := fmt.Sprintf("mr-out-%d", reduceID)
	tempFile, err := os.CreateTemp("./", fileName)
	if err != nil {
		log.Fatalf("Reduce Task: cannot create temp file %v", fileName)
	}

	sort.Sort(ByKey(kva))

	// Group the KeyValue pairs by key and apply the reduce function.
	// Write the output to the temporary file.
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// Write the output to the temporary file.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tempFile.Close()
	err = os.Rename(tempFile.Name(), fileName)
	if err != nil {
		log.Fatalf("Reduce Task: cannot rename temp file %v to %v", tempFile.Name(), fileName)
	}
	// Notify the coordinator that the task is complete.
	CallTaskComplete(taskID)
}

// Controller function for RPC AssignTask
func CallAssignTask() (*AssignTaskReply, error) {
	args := AssignTaskArgs{} // Example worker ID
	reply := AssignTaskReply{}
	call("Coordinator.AssignTask", &args, &reply)
	return &reply, nil
}

// Controller function for RPC TaskComplete
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
