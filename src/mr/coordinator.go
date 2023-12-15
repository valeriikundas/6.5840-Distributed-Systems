package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fixme: don't like absolute path of my pc, change to some temp dir
const TempDir = "/Users/user/Code/courses/distrubuted_systems_6.5840/src/main/mr-tmp"

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

	key string

	nReduce int
}

func (r ReduceTask) isInProgress() bool {
	return r.state == TaskStateInProgress
}

func (r ReduceTask) didTimeout() bool {
	return time.Since(r.startTime) > time.Second*10
}

func (r ReduceTask) getOutputFilePath() string {
	return getReduceFileName(r.id)
}

func (r ReduceTask) markIdle(c *Coordinator, i int) {
	c.mu.Lock()
	c.reduceTasks[i].state = TaskStateIdle
	c.reduceTasks[i].startTime = time.Time{}
	c.mu.Unlock()
}

func (r ReduceTask) markDone(c *Coordinator, i int) {
	c.mu.Lock()
	c.reduceTasks[i].state = TaskStateCompleted
	c.mu.Unlock()
}

type ShuffleState int

const (
	ShuffleStateNotStarted ShuffleState = iota
	ShuffleStateStarted
	ShuffleStateDone
)

type Coordinator struct {
	// Your definitions here.

	mu          sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask

	nReduce      int
	shuffleState ShuffleState
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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
			"found idle map task: i=%d id=%d type=%d key=%s  file=%s\n",
			i, response.ID, response.TaskType, response.ReduceKey, response.MapFile)

		c.mu.Lock()
		c.mapTasks[i].state = TaskStateInProgress
		c.mapTasks[i].startTime = startTime
		c.mu.Unlock()

		return nil
	}

	log.Printf("all map tasks are done")

	//fixme: recall on mutexes usage, are there problems with concurrency here?
	c.mu.Lock()
	if c.shuffleState == ShuffleStateNotStarted {
		c.shuffleState = ShuffleStateStarted

		c.shuffleIntermediateData()
		c.shuffleState = ShuffleStateDone
	}
	c.mu.Unlock()

	task, i = c.getIdleReduceTask()
	if task != nil {
		response.ID = task.ID
		response.TaskType = Reduce
		response.StartTime = startTime.UnixMicro()
		response.ReduceKey = task.ReduceKey
		response.NReduce = task.NReduce

		log.Printf(
			"found idle reduce task: i=%d id=%d type=%d key=%s",
			i, response.ID, response.TaskType, response.ReduceKey)

		c.mu.Lock()
		c.reduceTasks[i].state = TaskStateInProgress
		c.reduceTasks[i].startTime = startTime
		c.mu.Unlock()

		return nil
	}

	log.Printf("all reduce tasks are done")

	// FIXME: ending does not work as it should
	if c.Done() {
		return nil
	}
	return errors.New("idle task not found")
}

func (c *Coordinator) shuffleIntermediateData() {
	dirEntries, err := os.ReadDir(TempDir)
	if err != nil {
		log.Fatal(err)
	}

	// todo: implement external sort
	intermediateData := make([]KeyValue, 0)
	for _, entry := range dirEntries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") || !strings.HasPrefix(name, "mr-int") {
			continue
		}

		filepath := path.Join(TempDir, name)
		v, err := readJSONFile[[]KeyValue](filepath)
		if err != nil {
			log.Fatal(err)
		}

		intermediateData = append(intermediateData, *v...)
	}

	sort.Sort(ByKey(intermediateData))

	bucketID := 0
	bucketSize := len(intermediateData) / c.nReduce // todo: probably can `divide by len(c.mapTasks) additionally`
	for i := 0; i < len(intermediateData); {
		bucket := make([]KeyValue, 0, bucketSize)

		j := i + bucketSize + 1
		for ; j < len(intermediateData) && intermediateData[j].Key == intermediateData[i].Key; j += 1 {
		}

		bucket = append(bucket, intermediateData[i:j-1]...)

		//fixme: not working, has same word in neighbor files
		log.Printf("bucket indices %d %d", i, j)

		name := fmt.Sprintf("mr-sorted-%d.json", bucketID)
		bucketID += 1

		filepath := path.Join(TempDir, name)

		file, err := os.Create(filepath)
		if err != nil {
			log.Fatal(err)
		}

		bucketEncoder := json.NewEncoder(file)
		err = bucketEncoder.Encode(bucket)
		if err != nil {
			log.Fatal(err)
		}

		i = j
	}
}

func readJSONFile[R any](filepath string) (*R, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var v R
	err = decoder.Decode(&v)
	if err != nil {
		return nil, err
	}
	return &v, nil
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
			ID:        reduceTask.id,
			TaskType:  Reduce,
			ReduceKey: reduceTask.key,
			NReduce:   reduceTask.nReduce,
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
			log.Println("Map tasks are not done")
			return false
		}
	}
	log.Println("Map tasks are done")

	reduceTasksCompletion := make([]bool, len(c.reduceTasks))
	for i, task := range c.reduceTasks {
		reduceTasksCompletion[i] = task.state == TaskStateCompleted
	}

	for _, task := range c.reduceTasks {
		if task.state != TaskStateCompleted {
			log.Println("Reduce tasks are not done")
			return false
		}
	}
	log.Println("Reduce tasks are done")

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetFlags(log.Lshortfile)

	// todo: remove temp dir before starting

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

		// todo: should lock here?
		c.mapTasks = append(c.mapTasks, MapTask{
			id:       getNextID(),
			state:    TaskStateIdle,
			file:     file,
			contents: contents,
			nReduce:  nReduce,
		})
	}

	c.nReduce = nReduce

	/*
		create reduce tasks
		amount = nReduce
		filter by file name
	*/
	for i := 0; i < nReduce; i += 1 {
		reduceKey := strconv.Itoa(i)
		c.reduceTasks = append(c.reduceTasks, ReduceTask{
			id:        i,
			state:     TaskStateIdle,
			startTime: time.Time{},
			key:       reduceKey,
			nReduce:   nReduce,
		})
	}

	// goroutine for checking timed out and done tasks
	go func() {
		for {
			for i, t := range c.mapTasks {
				checkTimeout(&c, t, i)
			}

			for i, t := range c.reduceTasks {
				checkTimeout(&c, t, i)
			}

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

		// todo: delete mr-tmp completely

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

// fixme: how to name interfaces?
type ITask interface {
	isInProgress() bool
	didTimeout() bool
	getOutputFilePath() string
	markIdle(c *Coordinator, i int)
	markDone(c *Coordinator, i int)
}

func (t MapTask) isInProgress() bool {
	isInProgress := t.state == TaskStateInProgress

	if isInProgress && t.startTime.IsZero() {
		panic("startTime is zero, but should not be")
	}

	return isInProgress
}

func (t MapTask) didTimeout() bool {
	return time.Since(t.startTime) > time.Second*10
}

func (t MapTask) getOutputFilePath() string {
	return getIntermediateFilePath(t.id, t.file, t.nReduce)
}

func (t MapTask) markIdle(c *Coordinator, i int) {
	c.mu.Lock()
	c.mapTasks[i].state = TaskStateIdle
	c.mapTasks[i].startTime = time.Time{}
	c.mu.Unlock()
}

func (t MapTask) markDone(c *Coordinator, i int) {
	c.mu.Lock()
	c.mapTasks[i].state = TaskStateCompleted
	c.mu.Unlock()
}

func checkTimeout[T ITask](c *Coordinator, t T, i int) {
	if t.isInProgress() {
		taskTimedOut := t.didTimeout()
		if taskTimedOut {
			t.markIdle(c, i)
			return
		}

		// todo: implement different completion conditions for map and reduce task
		filepath := t.getOutputFilePath()
		_, err := os.Stat(filepath)
		if err != nil {
			if os.IsNotExist(err) {
				slog.Debug("file does not exist", "error", err, "filepath", filepath)
				log.Printf("file does not exist %s", filepath)
			} else {
				log.Printf("error checking file %s. details: %s", filepath, err)
			}

			return
		}

		t.markDone(c, i)
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

	reduceTaskID := 0
	// todo: when intermediate files will be split into nReduce files, then this should be uncommented
	//reduceTaskID := ihash(file) % nReduce

	filename := fmt.Sprintf("mr-int-%d-%d.json", mapTaskID, reduceTaskID)
	filepath := path.Join(TempDir, filename)
	return filepath
}

var gID = 0

func getNextID() int {
	gID++
	return gID
}
