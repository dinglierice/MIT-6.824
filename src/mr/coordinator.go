package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Unassigned TaskStatus = iota
	InProgress
	Finished
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Done
)

// 定义基本的数据结构
type Task struct {
	taskType    TaskType
	taskId      int
	inputFiles  []string
	taskStatus  TaskStatus
	mapCount    int
	reduceCount int

	startTime time.Time
}

type Coordinator struct {
	files       []string
	reduceCount int
	mapCount    int

	mapTasks    []Task // map任务列表
	reduceTasks []Task // reduce任务列表

	mapFinished    int
	reduceFinished int
	phase          TaskType
	done           bool

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask
// worker向master请求任务的方法
func (c *Coordinator) RequestTask(args *RequestTaskArg, reply *RequestTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.phase == Map {
		for i, task := range c.mapTasks {
			if task.taskStatus == Unassigned {
				c.mapTasks[i].taskStatus = InProgress
				c.mapTasks[i].startTime = time.Now()
				reply.task = task
				return nil
			}
		}
		// 没有可用的Map任务，等待
		reply.task = Task{taskType: Wait}
	} else if c.phase == Reduce {
		for i, task := range c.reduceTasks {
			if task.taskStatus == Unassigned {
				c.reduceTasks[i].taskStatus = InProgress
				c.reduceTasks[i].startTime = time.Now()
				reply.task = task
				return nil
			}
		}
		// 没有可用的Reduce任务，等待
		reply.task = Task{taskType: Wait}
	} else {
		// 所有任务完成，让worker退出
		reply.task = Task{taskType: Done}
	}

	return nil
}

// ReportTask
// worker向master报告任务状态的方法
func (c *Coordinator) ReportTask(args *ReportTaskArg, reply *ReportTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := args.task
	if task.taskType == Map {
		// 修改任务状态
		if c.mapTasks[task.taskId].taskStatus == InProgress {
			c.mapTasks[task.taskId].taskStatus = Finished
			c.mapFinished++

			// 检查是否所有Map任务都完成
			if c.mapFinished == c.mapCount {
				c.phase = Reduce
			}
		}
	} else if task.taskType == Reduce {
		if c.reduceTasks[task.taskId].taskStatus == InProgress {
			c.reduceTasks[task.taskId].taskStatus = Finished
			c.reduceFinished++

			// 检查是否所有Reduce任务都完成
			if c.reduceFinished == c.reduceCount {
				c.phase = Done
				c.done = true
			}
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// 1. 注册coordinator实例为RPC服务
	rpc.Register(c)
	// 2. 设置HTTP处理器
	rpc.HandleHTTP()
	// 3. 创建Unix域套接字监听
	sockname := coordinatorSock()        // 生成socket文件路径
	os.Remove(sockname)                  // 清理旧的socket文件
	l, e := net.Listen("unix", sockname) // 监听Unix socket
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// 4. 启动HTTP服务器处理RPC请求
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.done
}

func (c *Coordinator) checkTimeout() {
	for {
		// 每秒检查一次全局任务状态
		time.Sleep(time.Second)
		c.mutex.Lock()

		if c.phase == Map {
			for i, task := range c.mapTasks {
				if task.taskStatus == InProgress {
					// 超时的部分重置为未分配
					if time.Since(task.startTime) > 10*time.Second {
						c.mapTasks[i].taskStatus = Unassigned
					}
				}
			}
		} else if c.phase == Reduce {
			for i, task := range c.reduceTasks {
				if task.taskStatus == InProgress {
					if time.Since(task.startTime) > 10*time.Second {
						c.reduceTasks[i].taskStatus = Unassigned
					}
				}
			}
		}

		c.mutex.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		reduceCount: nReduce,
		mapCount:    len(files),
		phase:       Map,
		done:        false,
	}

	// Your code here.
	// 将files分配给不同workers
	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i] = Task{
			taskType:    Map,
			taskId:      i,
			inputFiles:  []string{file},
			reduceCount: nReduce,
			mapCount:    len(files),
			taskStatus:  Unassigned,
		}
	}

	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			taskType:    Reduce,
			taskId:      i,
			reduceCount: nReduce,
			mapCount:    len(files),
			taskStatus:  Unassigned,
		}
	}
	// 接受workers的信号，直到所有的map任务都完成
	go c.checkTimeout()

	c.server()
	return &c
}
