package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Task struct {    //master返回给worker的任务，注意对外（别的文件）暴露的字段，首字母要大写
	Operation string    //应该做什么，map，reduce或者merge
	MapFilename string    //若是map操作，该字段表示应该对哪个文件进行map操作
	ReduceFilenames []string    //若是reduce操作，该字段表示应该对那个后缀数字相同的文件簇进行reduce操作
	ReduceNo int    //若是reduce操作，该字段表示当前是第几个reduce任务
	FinalFilenames []string    //若是merge操作，该字段表示应该对哪些文件进行merge操作
}




// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
