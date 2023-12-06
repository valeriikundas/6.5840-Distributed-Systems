package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const TempDir = "./mr-tmp"

type TaskState int

const (
	TaskStateIdle TaskState = iota
	TaskStateInProgress
	TaskStateCompleted
)

type MapTask struct {
	id        int
	state     TaskState
	startTime time.Time

	file     string
	contents string

	nReduce int
}

type ReduceTask struct {
	id        int
	state     TaskState
	startTime time.Time

	key    string
	values []string

	nReduce int
}

// type Intermediate struct {
// 	location string
// 	size     int
// }

// type WorkerStruct struct {
// }

type Coordinator struct {
	// Your definitions here.

	// FIXME: add mutexes for shared data editing
	mu          sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask

	// NOTE: will be stored on disk
	// intermediateFiles Intermediate

	// workers WorkerStruct
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskRequest(args *Args, response *Task) error {
	startTime := time.Now()

	// FIXME: feels awkward to pass position, think if it's ok
	task, i := c.getIdleMapTask()
	if task != nil {
		response.ID = task.ID
		response.TaskType = Map
		response.StartTime = startTime.UnixMicro()
		response.MapFile = task.MapFile
		response.MapContents = task.MapContents
		response.NReduce = task.NReduce

		log.Printf(
			"found idle map task: i=%d id=%d type=%d key=%s values=%v file=%s\n",
			i, response.ID, response.TaskType, response.ReduceKey,
			response.ReduceValues, response.MapFile)

		c.mu.Lock()
		c.mapTasks[i].state = TaskStateInProgress
		c.mapTasks[i].startTime = startTime
		c.mu.Unlock()

		return nil
	}

	log.Printf("all map tasks are done")

	task, i = c.getIdleReduceTask()
	if task != nil {
		response.ID = task.ID
		response.TaskType = Reduce
		response.StartTime = startTime.UnixMicro()
		response.ReduceKey = task.ReduceKey
		response.ReduceValues = task.ReduceValues
		response.NReduce = task.NReduce

		log.Printf(
			"found idle reduce task: i=%d id=%d type=%d key=%s values=%v file=%s\n",
			i, response.ID, response.TaskType, response.ReduceKey,
			response.ReduceValues, response.MapFile)

		c.mu.Lock()
		c.reduceTasks[i].state = TaskStateInProgress
		c.reduceTasks[i].startTime = startTime
		c.mu.Unlock()

		return nil
	}

	// FIXME: ending does not work as it should
	if c.Done() {
		return nil
	}
	return errors.New("idle task not found")
}

func (c *Coordinator) getIdleMapTask() (*Task, int) {
	idleMapTaskIndex := -1
	for i := 0; i < len(c.mapTasks); i += 1 {
		if c.mapTasks[i].state == TaskStateIdle {
			idleMapTaskIndex = i
			break
		}
	}
	if idleMapTaskIndex != -1 {
		mapTask := c.mapTasks[idleMapTaskIndex]
		return &Task{
			ID:          mapTask.id,
			TaskType:    Map,
			MapFile:     mapTask.file,
			MapContents: mapTask.contents,
			NReduce:     mapTask.nReduce,
		}, idleMapTaskIndex
	}
	return nil, 0
}

func (c *Coordinator) getIdleReduceTask() (*Task, int) {
	idleReduceTaskIndex := -1
	for i := 0; i < len(c.reduceTasks); i += 1 {
		if c.reduceTasks[i].state == TaskStateIdle {
			idleReduceTaskIndex = i
			break
		}
	}
	if idleReduceTaskIndex != -1 {
		reduceTask := c.reduceTasks[idleReduceTaskIndex]
		return &Task{
			ID:           reduceTask.id,
			TaskType:     Reduce,
			ReduceKey:    reduceTask.key,
			ReduceValues: reduceTask.values,
			NReduce:      reduceTask.nReduce,
		}, idleReduceTaskIndex
	}
	return nil, 0
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	for _, t := range c.mapTasks {
		if t.state != TaskStateCompleted {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetFlags(log.Lshortfile)

	if _, err := os.Stat(TempDir); os.IsNotExist(err) {
		err = os.MkdirAll(TempDir, 0744)
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, file := range files {
		bytes, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("error opening file. details: %s", err)
		}

		contents := string(bytes)

		c.mapTasks = append(c.mapTasks, MapTask{
			id:       getNextID(),
			state:    TaskStateIdle,
			file:     file,
			contents: contents,
			nReduce:  nReduce,
		})
	}

	/*
		create reduce tasks
		nReduce
		filter by file name
	*/
	for i := 0; i < nReduce; i += 1 {
		dirEntries, err := os.ReadDir(TempDir)
		if err != nil {
			log.Fatal(err)
		}

		var curReduceTaskFiles []string
		for _, entry := range dirEntries {

			filename := entry.Name()
			if strings.HasSuffix(filename, fmt.Sprintf("%d.json", i)) {
				curReduceTaskFiles = append(curReduceTaskFiles, filename)
			}
		}

		log.Printf("len(curReduceTaskFiles)=%d, %v", len(curReduceTaskFiles), curReduceTaskFiles)

		for _, reduceTaskFile := range curReduceTaskFiles {
			b, err := os.ReadFile(reduceTaskFile)
			if err != nil {
				log.Fatal(err)
			}

			var kva []struct {
				Key, Value string
			}
			err = json.Unmarshal(b, &kva)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("kva=%+v", kva)
			break
		}

		reduceKey := strconv.Itoa(i) //todo:
		reduceValues := []string{}   //todo:
		c.reduceTasks = append(c.reduceTasks, ReduceTask{
			id:        i,
			state:     TaskStateIdle,
			startTime: time.Time{},
			key:       reduceKey,
			values:    reduceValues,
			nReduce:   nReduce,
		})
		log.Printf("created reduce task #%d", i)
	}

	// goroutine for checking timed out and done tasks
	go func() {
		for {
			for i, t := range c.mapTasks {
				checkTimeout(&c, t, i)
			}

			// for i, t := range c.reduceTasks {
			// 	checkTimeout(&c, t, i)
			// }

			time.Sleep(time.Second)
		}
	}()

	// TODO: check that timeouts work
	// TODO: when all map tasks is done, continue
	// TODO: sort by keys
	// TODO: implement reduce

	log.Println("server starting")
	c.server()

	defer func() {
// clean

		dirEntries, err := os.ReadDir(TempDir)
		if err != nil {
			log.Fatal(err)
		}

		tobedeleted := []string{}
		for _, e := range dirEntries {
			if strings.HasPrefix(e.Name(), "mr-int-") {
				tobedeleted = append(tobedeleted, e.Name())
			}
		}

		for _, name := range tobedeleted {
			err = os.Remove(path.Join("mr-tmp/", name))
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	return &c
}

type TaskInterface interface{}

func checkTimeout(c *Coordinator, t MapTask, i int) {
	if t.state == TaskStateInProgress {
		if t.startTime.IsZero() {
			log.Fatalf("start time should not be nil here : id=%d state=%d start=%v", t.id, t.state, t.startTime)
		}

		// log.Printf("start=%v time since=%v\n", t.startTime, time.Since(t.startTime))

		// task timed out
		if time.Since(t.startTime) > time.Second*10 {
			// log.Printf("map task (%d,%s) timed out", t.id, t.file)
			c.mu.Lock()
			c.mapTasks[i].state = TaskStateIdle
			c.mapTasks[i].startTime = time.Time{}
			c.mu.Unlock()
			return
		}

		// todo: implement different completion conditions for map and reduce task

		filepath := getIntermediateFilePath(t.id, t.file, t.nReduce)

		_, err := os.Stat(filepath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("file does not exist %s", filepath)
			} else {
				log.Printf("error checking file %s. details: %s", filepath, err)
			}

			return
		}

		// intermediate file exists -> task considered done
		// log.Printf("map task (%d,%s) completed", t.id, t.file)
		c.mu.Lock()
		c.mapTasks[i].state = TaskStateCompleted
		c.mu.Unlock()

	}
}

func assertMapTasksAreDone(c *Coordinator) {
	allMapTasksDone := false
	for _, t := range c.mapTasks {
		log.Printf("map task (%d,%s) state=%d", t.id, t.file, t.state)
		if t.state == TaskStateCompleted {
			allMapTasksDone = true
		} else {
			allMapTasksDone = false
			break
		}
	}

	if !allMapTasksDone {
		panic("all map tasks should be done by now")
	}

}

func getIntermediateFilePath(id int, file string, nReduce int) string {
	mapTaskID := id
	reduceTaskID := ihash(file) % nReduce
	filename := fmt.Sprintf("mr-int-%d-%d.json", mapTaskID, reduceTaskID)
	filepath := path.Join(TempDir, filename)
	return filepath
}

var gID = 0

func getNextID() int {
	gID++
	return gID
}
