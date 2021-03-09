package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	ConstTaskWait = 0
	ConstTaskMap = 1
	ConstTaskReduce = 2
	ConstStateIdle = 0
	ConstStateProcessing = 1
	ConstStateDone = 2
)

type Status struct{
	last_time int64
	state int
}

type Master struct {
	// Your definitions here.
	mapTask map[string]Status
	reduceTask map[int]Status
	fileId map[string]int
	mapCount, reduceCount, fileCount int
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m* Master) GetTask(args *int, reply *GetTaskReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.mapTask{
		if v.state == ConstStateIdle{
			m.mapTask[k] = Status{state: ConstStateProcessing, last_time: time.Now().Unix()}
			*reply = GetTaskReply{TaskType: ConstTaskMap, FileName: k, FileId: m.fileId[k]}
			//fmt.Println(*reply)
			return nil
		}
	}

	if m.mapCount < m.fileCount{
		*reply = GetTaskReply{TaskType: ConstTaskWait}
		return nil
	}

	for k, v := range m.reduceTask{
		if v.state == ConstStateIdle{
			m.reduceTask[k] = Status{state: ConstStateProcessing, last_time: time.Now().Unix()}
			*reply = GetTaskReply{TaskType: ConstTaskReduce, ReduceIndex: k, TotalFile: m.fileCount}
			//fmt.Println(*reply)
			return nil
		}
	}

	*reply = GetTaskReply{TaskType: ConstTaskWait}
	return nil
}

func (m *Master) DoneTask(args *DoneTaskArgs, reply *int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	//fmt.Println(*args)
	if args.TaskType == ConstTaskMap{
		m.mapTask[args.FileName] = Status{state: ConstStateDone}
		m.mapCount++
	} else {
		m.reduceTask[args.ReduceIndex] = Status{state: ConstStateDone}
		m.reduceCount++
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// First check die task in Done()
	timeNow := time.Now().Unix()
	for fileName, taskStatus := range m.mapTask{
		if taskStatus.state == ConstStateProcessing && timeNow - 10 >= taskStatus.last_time{
			m.mapTask[fileName] = Status{last_time: 0, state: ConstStateIdle}
		}
	}
	for index, taskStatus := range m.reduceTask{
		if taskStatus.state == ConstStateProcessing && timeNow - 10 >= taskStatus.last_time{
			m.reduceTask[index] = Status{last_time: 0, state: ConstStateIdle}
		}
	}

	// Master done if all task done
	return m.reduceCount == 10 && m.mapCount == m.fileCount
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapTask = make(map[string]Status)
	m.reduceTask = make(map[int]Status)
	m.fileId = make(map[string]int)
	m.fileCount = 0

	for _, fileName := range files{
		m.mapTask[fileName] = Status{}
		m.fileId[fileName] = m.fileCount
		m.fileCount++
	}
	for i := 0; i < nReduce; i++{
		m.reduceTask[i] = Status{}
	}
	m.server()
	return &m
}
