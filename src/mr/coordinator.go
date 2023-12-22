package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fixme: don't like absolute path of my pc, change to some temp dir
const SrcDir = "/Users/user/Code/courses/distrubuted_systems_6.5840/src/"
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
	return getExistingTempReduceFileName(r.id)
}

func getExistingTempReduceFileName(taskID int) string {
	return filepath.Join(TempDir, fmt.Sprintf("mr-temp-out-%d", taskID))
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

	nReduce                int
	shuffleState           ShuffleState
	renamedTempReduceFiles bool
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

	// todo: should it be wrapped with mutex lock?
	// todo: this code block runs multiple times (for each task request), but
	// one is enough. can refactor to extra process in coordinator that checks
	// for map and reduce tasks completeness, timeouts and shuffling data
	for {
		debug("waiting for map tasks to be done")
		done := true
		for _, mapTask := range c.mapTasks {
			if mapTask.state != TaskStateCompleted {
				done = false
				break
			}
		}
		if done {
			debug("all map tasks are done")
			break
		}
		time.Sleep(time.Second)
	}

	//fixme: recall on mutexes usage, are there problems with concurrency here?
	c.mu.Lock()
	if c.shuffleState == ShuffleStateNotStarted {
		c.shuffleState = ShuffleStateStarted

		c.shuffleIntermediateData()
		c.shuffleState = ShuffleStateDone
	}
	c.mu.Unlock()

	// fixme: lock unlock looks strange, will be fixed when above code moved to separate goroutine

	c.mu.Lock()
	// todo: why is lock added here?
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

		c.reduceTasks[i].state = TaskStateInProgress
		c.reduceTasks[i].startTime = startTime

		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// todo: should it be wrapped with mutex lock?
	for {
		debug("waiting for reduce tasks to be done")
		done := true
		for _, reduceTask := range c.reduceTasks {
			if reduceTask.state != TaskStateCompleted {
				done = false
				break
			}
		}
		debug("check if done")
		if done {
			debug("all reduce tasks are done")
			break
		}
		debug("time sleep")
		time.Sleep(time.Second)
	}

	// next: this is not getting called. why?
	debug("rename")
	log.Print("renaming final reduce file versions...")
	dirEntries, err := os.ReadDir(TempDir)
	if err != nil {
		log.Fatal(err)
	}
	debug("direntries=%+v", dirEntries)

	for _, entry := range dirEntries {
		if !strings.HasPrefix(entry.Name(), "mr-temp-out") {
			continue
		}

		oldPath := filepath.Join(TempDir, entry.Name())

		newName := strings.Replace(entry.Name(), "-temp", "", 1)
		newPath := filepath.Join(TempDir, newName)

		debug("rename reduce file: %v to %v", oldPath, newPath)

		err := os.Rename(oldPath, newPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	// next: this code is not getting called. why?
	c.mu.Lock()
	debug("c.renamedTempReduceFiles = true")
	c.renamedTempReduceFiles = true
	c.mu.Unlock()
	// fixme: should move many code from TaskRequest to separate process in coordinator

	// fixme: next line is not printed out. why?
	debug("afterwards: reduce tasks done")
	log.Print("all reduce tasks are done")

	if c.Done() {
		debug("return nil")
		return nil
	}

	log.Print("no idle task found. all are in progress")
	// todo: probably better to send special signal that will have a meaning of
	// maps done / reduces done / all are in progress
	return nil
}

func (c *Coordinator) shuffleIntermediateData() {
	dirEntries, err := os.ReadDir(TempDir)
	if err != nil {
		log.Fatal(err)
	}

	debug("dirEntries=%v", dirEntries)

	// todo: implement external sort
	intermediateData := make([]KeyValue, 0)
	for _, entry := range dirEntries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") || !strings.HasPrefix(name, "mr-int") {
			continue
		}

		filepath := filepath.Join(TempDir, name)
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

		initialJ := i + bucketSize
		j := min(initialJ, len(intermediateData))
		for j < len(intermediateData) && intermediateData[j].Key == intermediateData[initialJ].Key {
			j += 1
		}

		bucket = append(bucket, intermediateData[i:j]...)

		log.Printf("bucket indices %d %d", i, j)

		name := fmt.Sprintf("mr-sorted-%d.json", bucketID)
		bucketID += 1

		filepath := filepath.Join(TempDir, name)
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

// fixme: can rewrite these functions to `remainingTasks` array field in coordinator
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
			// todo: create custom logger, add log everything to it, don't use default logger
			//log.Println("Map tasks are not done")
			return false
		}
	}
	log.Println("Map tasks are done")

	for _, task := range c.reduceTasks {
		if task.state != TaskStateCompleted {
			log.Println("Reduce tasks are not done")
			return false
		}
	}
	log.Println("Reduce tasks are done")
	debug("c.renamedTempReduceFiles=%v", c.renamedTempReduceFiles)

	if !c.renamedTempReduceFiles {
		log.Printf("has not renamed temp reduce files to final names")
		return false
	}

	log.Print("log done bye")
	debug("i'm done. bye..")

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	setupLogging("coordinator")

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

	log.Println("server starting")
	c.server()

	return &c
}

const debugEnabled = true

func debug(format string, v ...any) {
	if !debugEnabled {
		return
	}

	formatString := fmt.Sprintf("DEBUG: %s", format)
	err := log.Output(2, fmt.Sprintf(formatString, v...))
	must(err)

	//fmt.Printf(fmt.Sprintf("DEBUG: %s\n", format), v...)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func setupLogging(kind string) {
	subDirName := kind[0:1]
	logFolderPath := filepath.Join(SrcDir, "logs", subDirName)

	err := os.MkdirAll(logFolderPath, 0700)
	if err != nil {
		log.Fatal(err)
	}

	randNumber := rand.Int()

	logFileTime := time.Now().Format("2006_01_02_15_04_05")

	logFileName := fmt.Sprintf("%s_%d.log", logFileTime, randNumber)
	logFilePath := filepath.Join(logFolderPath, logFileName)
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.SetFlags(log.Lshortfile | log.Ltime)

	logWriters := []io.Writer{logFile}

	if debugEnabled {
		logWriters = append(logWriters, os.Stdout)
	}

	multiWriter := io.MultiWriter(logWriters...)
	log.SetOutput(multiWriter)
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

		outputFilepath := t.getOutputFilePath()
		_, err := os.Stat(outputFilepath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("Reduce file does not exist, so task is not finished")
			} else {
				log.Printf("error checking file %s. details: %s", outputFilepath, err)
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
	filePath := filepath.Join(TempDir, filename)
	return filePath
}

var gID = 0

func getNextID() int {
	gID++
	return gID
}
