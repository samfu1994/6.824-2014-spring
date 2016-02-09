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
func (mr *MapReduce) send_map(id int, availbaleChannel string) bool{
          var reply DoJobReply;  
          arg := DoJobArgs{File:mr.file, Operation: Map, JobNumber:id, NumOtherPhase:mr.nReduce}
          ok := call(availbaleChannel, "Worker.DoJob", arg, &reply)
          if ok == false{
            fmt.Println("can't do map");
          }
          return ok;
}
func (mr *MapReduce) send_reduce(id int, reduceChannel string) bool{
          var reply DoJobReply;  
          arg := DoJobArgs{File:mr.file, Operation: Reduce, JobNumber:id, NumOtherPhase:mr.nMap}
          ok := call(reduceChannel, "Worker.DoJob", arg, &reply)
          if ok == false{
            fmt.Println("can't do reduce");
          }
          return ok;
}

func (mr *MapReduce) RunMaster() *list.List {

  var barrierMap = make(chan int, mr.nMap)
  var barrierReduce = make(chan int, mr.nReduce);
  var ok bool;
  for i := 0; i < mr.nMap; i++{
    go func(id int){
      for{
        ok = false;
        var availbaleChannel string;
        select{
          case availbaleChannel = <- mr.idleChannel:
              ok = mr.send_map(id, availbaleChannel)
          case availbaleChannel = <- mr.registerChannel:
              ok = mr.send_map(id, availbaleChannel)
        }
        if ok{ 
          barrierMap <- id;
          mr.idleChannel <- availbaleChannel;
          return
        }
      }
    }(i)
  }
  for i := 0; i < mr.nMap; i++{
    <- barrierMap
  }


  for i := 0; i < mr.nReduce; i++{
    go func(id int){
      for{
        var reduceChannel string;
        ok = false;
        select{
        case reduceChannel = <- mr.idleChannel:
          ok = mr.send_reduce(id, reduceChannel)
        case reduceChannel = <- mr.registerChannel:
          ok = mr.send_reduce(id, reduceChannel)
        }
        if ok{
          barrierReduce <- id;
          mr.idleChannel <- reduceChannel
          return
        }
      }
    }(i)
  }
  for i:= 0; i < mr.nReduce; i++{
    <- barrierReduce
  }
  return mr.KillWorkers()
}
