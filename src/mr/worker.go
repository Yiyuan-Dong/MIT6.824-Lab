package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func SafelyOpen(filename string) *os.File {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	return file
}

func SafelyCreateOpen(filename string) *os.File {
	// Use O_TRUNC, seems I do not need to use atomic rename...
	file, err := os.OpenFile(filename, os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0777)
	if err != nil {
		log.Fatalf("cannot create-open %v", filename)
	}
	return file
}

func SafelyTempFile(filename string) *os.File {
	file, err := ioutil.TempFile("./", filename)
	if err != nil{
		log.Fatalf("cannot create temp file %v", filename)
	}
	return file
}

func SafelyReadALl(file *os.File, filename string) []byte {
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return content
}

func SafelyClose(file *os.File, filename string) {
	err := file.Close()
	if err != nil{
		log.Fatalf("cannot close %v", filename)
	}
}

func SafelyRename(oldname string, newname string){
	err := os.Rename(oldname, newname)
	if err != nil{
		log.Fatal("cannot rename %v to %v", oldname, newname)
	}
}

func ProcessMap(filename string, mapf func(string, string) []KeyValue, fileId int) {
	// Open file
	//fmt.Printf("Map! %v\n", fileId)
	file := SafelyOpen(filename)
	content := SafelyReadALl(file, filename)
	SafelyClose(file, filename)
	// Process map function
	kva := mapf(filename, string(content))
	kvaDivide := make([][]KeyValue, 10)
	for i := 0; i < 10; i++{
		kvaDivide[i] = make([]KeyValue, 0)
	}
	for _, v := range kva{
		hashValue := ihash(v.Key) % 10
		kvaDivide[hashValue] = append(kvaDivide[hashValue], v)
	}
	// Write result into file, should create new file
	for i := 0; i < 10; i++{
		filename = "mr-" + strconv.Itoa(fileId) + "-" + strconv.Itoa(i)
		file = SafelyTempFile(filename)
		enc := json.NewEncoder(file)
		for _, kv := range kvaDivide[i] {
			err := enc.Encode(&kv)
			if err != nil{
				log.Fatal("Error while json encode key value pair")
			}
		}
		SafelyClose(file, filename)
		SafelyRename(file.Name(), filename)
	}
}

func ProcessReduce(reduceIndex int, reducef func(string, []string) string, totalFile int) {
	//fmt.Printf("Reduce: %v\n", reduceIndex)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < totalFile; i++{
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIndex)
		file := SafelyOpen(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + "-" + strconv.Itoa(reduceIndex)
	ofile := SafelyTempFile(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	SafelyClose(ofile, oname)
	SafelyRename(ofile.Name(), oname)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	for {  // infinite loop until call() return false

		var getReply GetTaskReply
		var doneArgs DoneTaskArgs
		var temp int

		res := call("Master.GetTask", &temp, &getReply)
		//fmt.Println(getReply)
		if !res {  // The master terminate, means all tasks done
			fmt.Print("Worker will terminate")
			return
		}
		if getReply.TaskType == ConstTaskWait{  // sleep for 0.5 second, the loop again
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if getReply.TaskType == ConstTaskMap{
			ProcessMap(getReply.FileName, mapf, getReply.FileId)
			doneArgs = DoneTaskArgs{TaskType: ConstTaskMap, FileName: getReply.FileName}
			call("Master.DoneTask", &doneArgs, &temp)
		}
		if getReply.TaskType == ConstTaskReduce{
			ProcessReduce(getReply.ReduceIndex, reducef, getReply.TotalFile)
			doneArgs = DoneTaskArgs{TaskType: ConstTaskReduce, ReduceIndex: getReply.ReduceIndex}
			call("Master.DoneTask", &doneArgs, &temp)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
