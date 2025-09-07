package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// 1. 向coordinator请求任务
		task := requestTask()

		switch task.taskType {
		case Map:
			performMapTask(task, mapf)
			reportTask(task)
		case Reduce:
			performReduceTask(task, reducef)
			reportTask(task)
		case Wait:
			// 等待一秒后重新请求
			time.Sleep(time.Second)
		case Done:
			// 所有任务完成，退出
			return
		}
	}
}

// requestTask
// 请求任务
func requestTask() Task {
	args := RequestTaskArg{}
	reply := RequestTaskResponse{}
	call("Coordinator.RequestTask", &args, &reply)
	return reply.task
}

// reportTask
// 报告任务完成
func reportTask(task Task) {
	args := ReportTaskArg{task: task}
	reply := ReportTaskResponse{}

	call("Coordinator.ReportTask", &args, &reply)
}

// performMapTask
// 执行Map任务
func performMapTask(task Task, mapf func(string, string) []KeyValue) {
	// 读取输入文件
	filename := task.inputFiles[0]
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	// 调用Map函数
	kva := mapf(filename, string(content))

	// 创建中间文件，按reduce任务分桶
	buckets := make([][]KeyValue, task.reduceCount)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.reduceCount
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// 将每个桶写入对应的中间文件
	for i, bucket := range buckets {
		filename := fmt.Sprintf("mr-%d-%d", task.taskId, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}

		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		file.Close()
	}
}

// 执行Reduce任务
func performReduceTask(task Task, reducef func(string, []string) string) {
	// 读取所有相关的中间文件
	var kva []KeyValue

	for i := 0; i < task.mapCount; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.taskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 按key排序
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 创建输出文件
	outputFile := fmt.Sprintf("mr-out-%d", task.taskId)
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("cannot create %v", outputFile)
	}
	defer file.Close()

	// 对相同key的值进行reduce操作
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
