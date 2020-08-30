package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	//"io/ioutil"
	//"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// for sorting by key.
type ByKey []KeyValue


// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		time.Sleep(2 * time.Second)    //延缓worker的执行速度，方便多个worker并发执行
		reply := Task{}
		call("Master.Give", "ask", &reply)    //注意这里的reply一定要是指针
		if reply.Operation == "map" {

			intermediate := []KeyValue{}

			file, err := os.Open(reply.MapFilename)    //打开需要map操作的文件
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapFilename)
			}

			content, err := ioutil.ReadAll(file)    //读取文件内容
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapFilename)
			}
			file.Close()
			kva := mapf(reply.MapFilename, string(content))    //map操作
			intermediate = append(intermediate, kva...)    //将map操作之后得到的KeyValue序列放到intermediate切片

			//sort.Sort(ByKey(intermediate))

			interFilenames := make([]string, 10)    //把所有KeyValue通过ihash分散到reduceNum = 10个中间文件
			interFiles := make([]*os.File, 10)
			for i := 0; i < 10; i++ {
				interFilenames[i] = "mr-out-" + reply.MapFilename + "-" + strconv.Itoa(i)
				interFiles[i], _ = os.Create(interFilenames[i])    //创建10个文件
			}
			for i := 0; i < len(intermediate); i++{
					//写入到对应文件
				fmt.Fprintf(interFiles[ihash(intermediate[i].Key) % 10], "%v %v\n", intermediate[i].Key, intermediate[i].Value)
			}

			//ihash(key) % NReduce
			//写完，关闭文件
			for i := 0; i < 10; i++ {
				interFiles[i].Close()
			}

			//这里需要通过rpc通知master产生了新文件
			var reply string
			call("Master.GetInterFilenames", interFilenames, &reply)    //把文件名传过去给Master.getInterfilenames调用

		} else if reply.Operation == "reduce" {    //所有文件都已经写成kv对的形式，并且写到中间文件中
			//time.Sleep(time.Second)
			interFilenames := reply.ReduceFilenames   //拿到若干个序号相同的中间文件
			intermediate := []KeyValue{}

			//把在interfiles中的文件的内容提出来放在intermediate
			for i := 0; i < len(interFilenames); i++ {
				interFile, _ := os.Open(interFilenames[i])    //打开一个interfiles中的文件

				scanner := bufio.NewScanner(interFile)    //株行读取
				for scanner.Scan() {
					lineText := scanner.Text()
					strs := strings.Split(lineText, " ")
					intermediate = append(intermediate, KeyValue{strs[0], strs[1]})    //向intermdeiate添加KeyValue

				}

				interFile.Close()
			}

			//尾号为1的中间文件已经全部写入intermediate，接下来要把intermediate写入文件mr-out-1
			filename := "mr-out-final-" + strconv.Itoa(reply.ReduceNo)
			ofile, _ := os.Create(filename)

			sort.Sort(ByKey(intermediate))
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

			//这里需要通过rpc通知master产生了新文件
			var reply string
			call("Master.GetFinalFilenames", filename, &reply)

		} else if reply.Operation == "merge" {
			finalFilenames := reply.FinalFilenames
			intermediate := []KeyValue{}
			for i := 0; i < len(finalFilenames); i++ {
				finalFile, _ := os.Open(finalFilenames[i])    //打开一个interfiles中的文件

				scanner := bufio.NewScanner(finalFile)    //按行读取
				for scanner.Scan() {
					lineText := scanner.Text()
					strs := strings.Split(lineText, " ")
					intermediate = append(intermediate, KeyValue{strs[0], strs[1]})    //向intermdeiate添加KeyValue

				}

				finalFile.Close()
			}

			filename := "mr-out-0"
			ofile, _ := os.Create(filename)
			sort.Sort(ByKey(intermediate))

			i := 0
			for i < len(intermediate) {
				cnt := 0
				j := i
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					val, _ := strconv.Atoi(intermediate[j].Value)
					cnt += val
					j++
				}
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, strconv.Itoa(cnt))

				i = j
			}

			var reply string
			call("Master.GetFinalFilename", filename, &reply)
		} else {
			break
		}

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

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
	call("Master.Example", args, &reply)

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

	//fmt.Println(rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
