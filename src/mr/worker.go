package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path"
	"time"
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSequence(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.SetFlags(log.Lshortfile)

	if _, err := os.Stat(TempDir); os.IsNotExist(err) {
		err = os.MkdirAll(TempDir, 0744)
		if err != nil {
			log.Fatal(err)
		}
	}

	// repeatedly:
	//  - send a task request
	//  - receive completed task response
	//  - process task response (different for map and reduce functions)

	// Your worker implementation here.

	for {
		task := CallTaskRequest()
		if task == nil || task.IsZero() {
			// nil means all is done, exit
			return
		}
		if task.StartTime == 0 {
			log.Fatalf("task=%+v\n", task)
		}
		switch task.TaskType {
		case Map:
			log.Printf("worker received MAP task, id=%d, key=%s, values=%v, file=%s len(contents)=%d\n",
				task.ID, task.ReduceKey, task.ReduceValues, task.MapFile, len(task.MapContents))

			kva := mapf(task.MapFile, task.MapContents)

			bytes, err := json.Marshal(kva)
			if err != nil {
				log.Fatal(err)
			}

			// fixme: should split the output into nReduce separate files
			
			filepath := getIntermediateFilePath(task.ID, task.MapFile, task.NReduce)

			err = os.WriteFile(filepath, bytes, 0644)
			if err != nil {
				log.Fatal(err)
			}

		case Reduce:
			log.Printf("worker received REDUCE task, id=%d, key=%s, values=%v, file=%s len(contents)=%d \n",
				task.ID, task.ReduceKey, task.ReduceValues, task.MapFile, len(task.MapContents))

			key, values := task.ReduceKey, task.ReduceValues
			result := reducef(key, values)

			outputName := fmt.Sprintf("mr-out-%d", task.ID)
			outputPath := path.Join(TempDir, outputName)
			ofile, err := os.Create(outputPath)
			defer ofile.Close()
			if err != nil {
				log.Fatal(err)
			}

			fmt.Fprintf(ofile, "%v %v\n", key, result)
		}

		time.Sleep(time.Second)
	}
}

func CallTaskRequest() *Task {
	args := Args{}
	task := Task{}

	ok := call("Coordinator.TaskRequest", &args, &task)
	if ok {
		return &task
	} else {
		fmt.Println("rpc call failed")
		return nil
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
