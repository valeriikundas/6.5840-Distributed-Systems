package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sasha-s/go-deadlock"
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
	"time"
)

// fixme: don't like absolute path of my pc, change to some temp dir
const SrcDir = "/Users/user/Code/courses/distrubuted_systems_6.5840/src/"
const TempDir = "/Users/user/Code/courses/distrubuted_systems_6.5840/src/main/mr-tmp"

type TaskState int

const (
	TaskStateUndefined TaskState = iota
	TaskStateIdle
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

func (t MapTask) checkDone() bool {
	outputFilepath := t.getOutputFilePath()
	_, err := os.Stat(outputFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		} else {
			log.Fatalf("error checking file %s. details: %s", outputFilepath, err)
		}
	}
	return true
}

type ReduceTask struct {
	id        int
	state     TaskState
	startTime time.Time

	key string

	nReduce int
}

func (t ReduceTask) checkDone() bool {
	// todo: when reduce files are split, this will have to be adjusted to check all
	// files existence
	outputFilepath := t.getOutputFilePath()
	_, err := os.Stat(outputFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		} else {
			log.Fatalf("error checking file %s. details: %s", outputFilepath, err)
		}
	}
	return true
}

func (r ReduceTask) isInProgress() bool {
	return r.state == TaskStateInProgress
}

func (r ReduceTask) didTimeout() bool {
	return time.Since(r.startTime) > time.Second*10
}

func (r ReduceTask) getOutputFilePath() string {
	return getExistingTempReduceFilePath(r.id)
}

func getExistingTempReduceFilePath(taskID int) string {
	return filepath.Join(TempDir, getTempReduceFileName(taskID))
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

	mu          deadlock.Mutex
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
		// fixme: rewrite with *response=Task{...}
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

	if !c.areMapsDone() {
		response = nil
		return nil
	}

	for c.shuffleState != ShuffleStateDone {
		debug("maps done, shuffle is not")
		return nil
	}

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

	if !c.areReducesDone() {
		response = nil
		return nil
	}

	if c.Done() {
		*response = Task{
			TaskType:  EndSignal,
			StartTime: startTime.UnixMicro(),
		}
		return nil
	}

	// todo: probably better to send special signal that will have a meaning of
	// maps done / reduces done / all are in progress
	response = &Task{}
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

	if bucketID < len(c.reduceTasks) {
		c.mu.Lock()
		for i := bucketID; i < len(c.reduceTasks); i += 1 {
			c.reduceTasks[i].state = TaskStateCompleted
		}
		c.mu.Unlock()
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
		if c.reduceTasks[i].state == TaskStateUndefined {
			continue
		}
		if c.reduceTasks[i].state == TaskStateIdle {
			idleReduceTaskIndex = i
			break
		}
	}

	b := strings.Builder{}
	for _, task := range c.reduceTasks {
		b.WriteString(fmt.Sprintf("%v-%v; ", task.key, task.state))
	}
	debug("get task: reduce status: %v", b.String())

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

	for i, task := range c.reduceTasks {
		if task.state == TaskStateUndefined {
			continue
		}
		if task.state != TaskStateCompleted {
			log.Printf("Reduce tasks are not done: %v of %v", i, len(c.reduceTasks))
			return false
		}
	}

	if !c.renamedTempReduceFiles {
		log.Printf("has not renamed temp reduce files to final names")
		return false
	}

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

		c.mapTasks = append(c.mapTasks, MapTask{
			id:       getNextID(),
			state:    TaskStateIdle,
			file:     file,
			contents: string(bytes),
			nReduce:  nReduce,
		})
	}

	c.nReduce = nReduce

	// todo: reconsider creating reduce tasks here vs in controller goroutine
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

	ctx := context.Background()
	go c.runControlFlow(ctx)

	// TODO: check that timeouts work

	log.Println("server starting")
	c.server()

	return &c
}

func (c *Coordinator) runControlFlow(ctx context.Context) {
	iter := 0

	for {
		iter++

		// fixme: move this lower, check timeout after done check
		for i, t := range c.mapTasks {
			checkTaskStatus(c, t, i)
		}

		for i, t := range c.reduceTasks {
			checkTaskStatus(c, t, i)
		}

		// todo: should it be wrapped with mutex lock?
		mapsDone := c.areMapsDone()
		if mapsDone {
			// fixme: shuffleState seems not needed now
			if c.shuffleState == ShuffleStateNotStarted {
				c.shuffleState = ShuffleStateStarted
				c.shuffleIntermediateData()
				c.shuffleState = ShuffleStateDone
			}
		}

		b := strings.Builder{}
		for _, task := range c.reduceTasks {
			b.WriteString(fmt.Sprintf("%v-%v; ", task.key, task.state))
		}
		debug("controller reduce status: %v", b.String())

		reducesDone := c.areReducesDone()
		if reducesDone {
			debug("all reduce tasks are done")

			if !c.renamedTempReduceFiles {
				log.Print("renaming final reduce file versions...")
				renameTempReduceFiles()

				debug("c.renamedTempReduceFiles = true")
				c.renamedTempReduceFiles = true
			}
		}

		time.Sleep(time.Second)
	}
}

func (c *Coordinator) areReducesDone() bool {
	for _, reduceTask := range c.reduceTasks {
		if reduceTask.state == TaskStateUndefined {
			continue
		}
		if reduceTask.state != TaskStateCompleted {
			return false
		}
	}
	return true
}

func (c *Coordinator) areMapsDone() bool {
	for _, mapTask := range c.mapTasks {
		if mapTask.state != TaskStateCompleted {
			return false
		}
	}
	return true
}

func renameTempReduceFiles() {
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
}

const debugEnabled = false
const stdoutLogEnabled = false

func debug(format string, v ...any) {
	if !debugEnabled {
		return
	}

	err := log.Output(2, fmt.Sprintf(format, v...))
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

	if stdoutLogEnabled {
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
	checkDone() bool
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

func checkTaskStatus[T ITask](c *Coordinator, t T, i int) {
	if t.isInProgress() {
		taskTimedOut := t.didTimeout()
		if taskTimedOut {
			t.markIdle(c, i)
			return
		}

		if t.checkDone() {
			t.markDone(c, i)
		}
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
