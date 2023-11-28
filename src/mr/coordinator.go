package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
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
	log.Print("c.mapTasks=")
	for _, mt := range c.mapTasks {
		log.Printf("id=%d file=%s state=%d", mt.id, mt.file, mt.state)
	}

	// FIXME: feels awkward to pass position, think if it's ok
	task, i := c.getIdleMapTask()
	if task != nil {
		startTime := time.Now()
		log.Printf("startTime=%v", startTime)

		response.ID = task.ID
		response.TaskType = Map

		response.StartTime = startTime.UnixMicro()
		log.Printf("set response.StartTime=%v", response.StartTime)
		response.MapFile = task.MapFile
		response.MapContents = task.MapContents
		response.NReduce = task.NReduce

		log.Printf(
			"found idle map task: i=%d id=%d type=%d key=%s values=%v file=%s\n",
			i, response.ID, response.TaskType, response.ReduceKey,
			response.ReduceValues, response.MapFile)

		c.mu.Lock()
		c.mapTasks[i].state = InProgress
		c.mapTasks[i].startTime = startTime
		log.Printf("just set state=%v start=%v", c.mapTasks[i].state, c.mapTasks[i].startTime)
		c.mu.Unlock()

		return nil
	}

	task, i = c.getIdleReduceTask()
	if task != nil {
		response.ID = task.ID
		response.TaskType = Reduce
		response.ReduceKey = task.ReduceKey
		response.ReduceValues = task.ReduceValues
		response.NReduce = task.NReduce

		log.Printf(
			"found idle reduce task: i=%d id=%d type=%d key=%s values=%v file=%s\n",
			i, response.ID, response.TaskType, response.ReduceKey,
			response.ReduceValues, response.MapFile)

		c.reduceTasks[i].state = InProgress

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
		if c.mapTasks[i].state == Idle {
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
		if c.reduceTasks[i].state == Idle {
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
		if t.state != Completed {
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

	if _, err := os.Stat("./tmp"); os.IsNotExist(err) {
		err = os.MkdirAll("./tmp", 0744)
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
			state:    Idle,
			file:     file,
			contents: contents,
			nReduce:  nReduce,
		})
	}

	// goroutine for checking timed out and done tasks
	go func() {
		for {
			for i, t := range c.mapTasks {
				if t.state == InProgress {
					if t.startTime.IsZero() {
						log.Printf("id=%d state=%d start=%v", t.id, t.state, t.startTime)
						panic("start time should not be nil here")
					}

					log.Printf("start=%v time since=%v\n", t.startTime, time.Since(t.startTime))

					if time.Since(t.startTime) > time.Second*10 {
						log.Printf("map task (%d,%s) timed out", t.id, t.file)
						c.mu.Lock()
						c.mapTasks[i].state = Idle
						c.mapTasks[i].startTime = time.Time{}
						c.mu.Unlock()
					}

					filepath := getInterFilePath(t.id, t.file, t.nReduce)

					_, err := os.Stat(filepath)
					if err != nil {
						if os.IsNotExist(err) {
							log.Printf("file does not exist %s", filepath)
						} else {
							log.Printf("error checking file %s. details: %s", filepath, err)
						}

						continue
					}

					log.Printf("map task (%d,%s) completed", t.id, t.file)
					c.mu.Lock()
					c.mapTasks[i].state = Completed
					c.mu.Unlock()
				}
			}

			time.Sleep(time.Second)
		}
	}()

	log.Println("server starting")
	c.server()
	return &c
}

func getInterFilePath(id int, file string, nReduce int) string {
	mapTaskID := id
	reduceTaskID := ihash(file) % nReduce
	filename := fmt.Sprintf("mr-int-%d-%d.json", mapTaskID, reduceTaskID)
	filepath := path.Join("./tmp", filename)
	return filepath
}

var gID = 0

func getNextID() int {
	gID++
	return gID
}
