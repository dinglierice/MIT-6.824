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

// 修复：所有字段首字母大写
type Task struct {
	TaskType    TaskType   // 大写！
	TaskId      int        // 大写！
	InputFiles  []string   // 大写！
	TaskStatus  TaskStatus // 大写！
	MapCount    int        // 大写！
	ReduceCount int        // 大写！
	StartTime   time.Time  // 大写！
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
			if task.TaskStatus == Unassigned { // 使用大写字段名
				c.mapTasks[i].TaskStatus = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.Task = c.mapTasks[i] // 使用大写字段名
				return nil
			}
		}
		// 没有可用的Map任务，等待
		reply.Task = Task{TaskType: Wait} // 使用大写字段名
	} else if c.phase == Reduce {
		for i, task := range c.reduceTasks {
			if task.TaskStatus == Unassigned {
				c.reduceTasks[i].TaskStatus = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.Task = c.reduceTasks[i]
				return nil
			}
		}
		// 没有可用的Reduce任务，等待
		reply.Task = Task{TaskType: Wait}
	} else {
		// 所有任务完成，让worker退出
		reply.Task = Task{TaskType: Done}
	}

	return nil
}

// ReportTask
// worker向master报告任务状态的方法
func (c *Coordinator) ReportTask(args *ReportTaskArg, reply *ReportTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := args.Task // 使用大写字段名
	if task.TaskType == Map {
		// 修改任务状态
		if c.mapTasks[task.TaskId].TaskStatus == InProgress {
			c.mapTasks[task.TaskId].TaskStatus = Finished
			c.mapFinished++

			// 检查是否所有Map任务都完成
			if c.mapFinished == c.mapCount {
				c.phase = Reduce
			}
		}
	} else if task.TaskType == Reduce {
		if c.reduceTasks[task.TaskId].TaskStatus == InProgress {
			c.reduceTasks[task.TaskId].TaskStatus = Finished
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
				if task.TaskStatus == InProgress {
					// 超时的部分重置为未分配
					if time.Since(task.StartTime) > 10*time.Second {
						c.mapTasks[i].TaskStatus = Unassigned
					}
				}
			}
		} else if c.phase == Reduce {
			for i, task := range c.reduceTasks {
				if task.TaskStatus == InProgress {
					if time.Since(task.StartTime) > 10*time.Second {
						c.reduceTasks[i].TaskStatus = Unassigned
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
			TaskType:    Map,
			TaskId:      i,
			InputFiles:  []string{file},
			ReduceCount: nReduce,
			MapCount:    len(files),
			TaskStatus:  Unassigned,
		}
	}

	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskType:    Reduce,
			TaskId:      i,
			ReduceCount: nReduce,
			MapCount:    len(files),
			TaskStatus:  Unassigned,
		}
	}
	// 接受workers的信号，直到所有的map任务都完成
	go c.checkTimeout()

	c.server()
	return &c
}
