package mr

import (
	"encoding/json"
	"fmt"
	errors "github.com/pkg/errors"
	"hash/fnv"
	"log"
	"log/slog"
	"math/rand"
	"net/rpc"
	"os"
	"path"
	filepathLib "path/filepath"
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

	setupLogging("worker")

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
			log.Print("empty task received -> exiting...")
			return
		}
		if task.StartTime == 0 {
			log.Fatalf("task=%+v\n", task)
		}
		switch task.TaskType {
		case Map:
			log.Printf("worker received MAP task, id=%d, key=%s, file=%s len(contents)=%d\n",
				task.ID, task.ReduceKey, task.MapFile, len(task.MapContents))

			kva := mapf(task.MapFile, task.MapContents)
			// fixme: why does 'kva' here contain key='', I think it should be filtered out by strings.FieldsFunc in mapf

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
			log.Printf("worker received REDUCE task, id=%d, key=%s, file=%s len(contents)=%d \n",
				task.ID, task.ReduceKey, task.MapFile, len(task.MapContents))

			taskID := task.ID

			filepath := path.Join(TempDir, fmt.Sprintf("mr-sorted-%d.json", taskID))
			file, err := os.Open(filepath)
			if err != nil {
				slog.Debug("open file error", "error", err, "filepath", filepath)
				log.Fatal(err)
			}

			fileDecoder := json.NewDecoder(file)
			var keyValues []KeyValue
			err = fileDecoder.Decode(&keyValues)
			if err != nil {
				log.Fatal(err)
			}

			// todo: write to file once
			// buffer := new(bytes.Buffer)

			i := 0
			for i < len(keyValues) {
				keyValue := keyValues[i]
				j := i + 1
				for j < len(keyValues) && keyValues[j].Key == keyValue.Key {
					j += 1
				}

				var values []string
				for k := i; k < j; k++ {
					values = append(values, keyValues[k].Value)
				}

				result := reducef(keyValue.Key, values)

				//fixme: refactor to write to file once
				//buffer.WriteString(fmt.Sprintf("%v %v", keyValue.Key, result))

				reduceFilePath := getReduceFileName(task.ID)

				if keyValue.Key != "" {
					err = appendReduceFile(reduceFilePath, keyValue.Key, result)
					if err != nil {
						log.Fatal(err)
					}
				}

				i = j
			}

		}

		time.Sleep(time.Second)
	}
}

func getReduceFileName(taskID int) string {
	outputName := fmt.Sprintf("mr-out-%d", taskID)
	//outputPath := path.Join("..", outputName)
	outputPath := outputName
	return outputPath
}

func appendReduceFile(filepath string, key string, result string) error {
	absoluteFilePath, err := filepathLib.Abs(filepath)
	if err != nil {
		return err
	}

	// todo: check if it will work without absolute path
	file, err := os.OpenFile(absoluteFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return errors.Wrap(err, "OpenFile error")
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	_, err = fmt.Fprintf(file, "%v %v\n", key, result)
	if err != nil {
		return errors.Wrap(err, "file write error")
	}

	return nil
}

func CallTaskRequest() *Task {
	args := Args{}
	task := Task{}

	ok := call("Coordinator.TaskRequest", &args, &task)
	if ok {
		return &task
	} else {
		log.Print("rpc call failed")
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
		// WIP: handle ending better
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Print("rpc client call error", err)
	return false
}
