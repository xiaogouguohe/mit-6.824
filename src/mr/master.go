package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	OriFilenames []string;    //所有pg-*文件
	InterFilenames []([]string);    //通过map生成的中间文件
	FinalFilenames []string;    //通过reduce生成的文件，二维切片，其中FinalFilanames
	ReduceNum int;    //中间文件分几次reduce执行
	FinalFilename string;    //最后得到的文件名（mr-out-0）
	IsFinished bool;    //是否已经完成整个流程
	mtx sync.Mutex    //互斥锁
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Give(args string, reply *Task) error {    //记得加锁
	m.mtx.Lock()    //加锁
	defer m.mtx.Unlock()    //函数返回后解锁
	if len(m.OriFilenames) > 0 {    //还有pg-*文件未被map处理
		reply.Operation = "map"    //此时任务为map
		reply.MapFilename = m.OriFilenames[0]    //取出第一个pg-*文件
		m.OriFilenames = m.OriFilenames[1 : len(m.OriFilenames)]
		return nil

	} else if len(m.InterFilenames[0]) > 0 {    //所有pg-*文件都被map处理，还有中间文件没有被reduce处理
		reply.Operation = "reduce"    //此时任务为reduce
		reply.ReduceNo = len(m.FinalFilenames)    //是第几个reduce任务（FinalFinenames的长度表明之前已经执行了多少个reduce任务）
		for i := 0; i < len(m.InterFilenames); i++ {    //中间文件的格式为mr-out-*-X，其中X为0到9
			reply.ReduceFilenames = append(reply.ReduceFilenames, m.InterFilenames[i][0])    //取出所有后缀序号一样的中间文件，相同的Key只可能被映射到序号一样的中间文件
			m.InterFilenames[i] = m.InterFilenames[i][1:]
		}
		return nil

	} else if len(m.FinalFilenames) > 0 {    //所有中间文件都被reduce处理成FinalFilenames，现在要将FinalFilenames合并成FinalFilename
		reply.Operation = "merge"    //此时任务为merge
		reply.FinalFilenames = m.FinalFilenames
		m.FinalFilenames = m.FinalFilenames[0:0]
		return nil

	} else {
		reply.Operation = "end"    //告诉worker可以结束了

		return nil
	}
}

func (m *Master) GetInterFilenames(args []string, reply *string) error {    //master获取worker通过map生成了哪些中间文件，args就是这个worker生成的中间文件
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.InterFilenames = append(m.InterFilenames, args)    //添加中间文件
	*reply = "m.GetInterFilenames done"    //告诉worker，master已经获得了worker生成的中间文件的名字
	return nil
}

func (m *Master) GetFinalFilenames(args *string, reply *string) error {    //master获取worker通过reduce生成了哪些final文件，args就是这个worker生成的中间文件
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.FinalFilenames = append(m.FinalFilenames, *args)
	*reply = "m.GetFinalFilenames done"
	//fmt.Println(m.FinalFilenames)
	return nil
}

func (m *Master) GetFinalFilename(args *string, reply *string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.FinalFilename = *args
	*reply = "m.GetFinalFilename done"
	//fmt.Println(m.FinalFilename)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}*/



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
	//fmt.Println("aaa")
	go http.Serve(l, nil)
	//fmt.Println("bbb")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if (m.FinalFilename != "") {    //判断条件有问题，可能有些worker还没来得及收到Give的end信号，master就已经退出，导致worker被动断开连接
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.OriFilenames = files
	m.InterFilenames = make([]([]string), 0)
	m.FinalFilenames = make([]string, 0)
	m.ReduceNum = nReduce
	m.IsFinished = false
	m.FinalFilename = ""

	m.server()
	return &m
}
