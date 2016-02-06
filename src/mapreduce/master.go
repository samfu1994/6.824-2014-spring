package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  //should return only when all of the map and reduce tasks 
  //have been executed
  // var ntasks int
  // var nios int // number of inputs (for reduce) or outputs (for map)
  // switch phase {
  //   case mapPhase:
  //     ntasks = len(mr.files)
  //     nios = mr.nReduce
  //   case reducePhase:
  //     ntasks = mr.nReduce
  //     nios = len(mr.files)
  // }

  for i := 0; i < ntasks; i++{
    fmt.Println("now on TASK:#####", i)

    createWorker(mr.address, "worker"+strconv.Itoa(i), mapF, reduceF);//wc.go: mapF
    var regArg RegisterArgs;
    regArg.Worker = "worker"+strconv.Itoa(i)
    mr.Register(&regArg, new(struct{}));
    arg := DoTaskArgs{Phase: phase, TaskNumber: i, NumOtherPhase: nios}
    ok := call(mr.workers[i], "Worker.DoTask", arg, new(struct{}))
    if ok != false{
      fmt.Println("can't do task");
    }
  }
  return mr.KillWorkers()
}
