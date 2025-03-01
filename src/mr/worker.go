package mr

import (
	"encoding/json"
	"fmt"
	errors "github.com/pkg/errors"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
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
		err = os.MkdirAll(TempDir, 0666)
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
		// fixme: task.isZero() condition probably can never happen. delete?
		if task.IsEndSignal() {
			// end signal means all is done, exit
			log.Print("end signal received -> worker exiting...")
			return
		}
		if task == nil || task.IsZero() || task.StartTime == 0 {
			log.Print("no task received -> worker sleeping...")
			time.Sleep(time.Second)
			continue
		}

		// todo: alternatively can have a done file that shows done status for all tasks
		// so it's shared between controller and workers

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
				log.Fatal(fmt.Errorf("error writing to file: %w", err))
			}

		case Reduce:
			log.Printf("worker received REDUCE task, id=%d, key=%s, file=%s len(contents)=%d \n",
				task.ID, task.ReduceKey, task.MapFile, len(task.MapContents))

			taskID := task.ID

			filePath := filepath.Join(TempDir, fmt.Sprintf("mr-sorted-%d.json", taskID))
			file, err := os.Open(filePath)
			if err != nil {
				// no such task, skipping
				log.Print(fmt.Errorf("error opening file: %w", err))
				continue
			}

			fileDecoder := json.NewDecoder(file)
			var keyValues []KeyValue
			err = fileDecoder.Decode(&keyValues)
			if err != nil {
				log.Fatal(fmt.Errorf("error decoding json: %w", err))
			}

			// todo: write to file once
			// buffer := new(bytes.Buffer)

			//fixme: use buffer
			builder := strings.Builder{}

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

				i = j

				// todo: look into if it can be deleted
				if keyValue.Key == "" {
					continue
				}

				builder.WriteString(fmt.Sprintf("%v %v\n", keyValue.Key, result))
			}

			// todo: reduce task should create files similar to intermediate files e.g.
			// mr-temp-out-1-5, where numbers are reduce task id and map task id, task
			// completeness is when all this file exist
			tempReduceFileName := getTempReduceFileName(taskID)

			tempReduceFilePath := filepath.Join(TempDir, tempReduceFileName)
			err = writeReduceFile(tempReduceFilePath, builder.String())
			if err != nil {
				log.Fatal(fmt.Errorf("writeReduceFile error: %w", err))
			}
		}

		time.Sleep(time.Second)
	}
}

func getFinalReduceFileName(taskID string) string {
	outputName := fmt.Sprintf("mr-out-%s", taskID)
	//outputPath := filepath.Join("..", outputName)
	outputPath := outputName
	return outputPath
}

func getTempReduceFileName(taskID int) string {
	return fmt.Sprintf("mr-temp-out-%d", taskID)
}

func writeReduceFile(filePath string, content string) error {
	absoluteFilePath, err := filepath.Abs(filePath)
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

	_, err = fmt.Fprint(file, content)
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
	}

	log.Printf("rpc call failed, args=%+v, task=%+v", args, task)
	return nil
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

	log.Printf("rpc client call error, args=%+v, reply=%+v, err=%v\n", args, reply, err)
	return false
}
