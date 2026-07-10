# Preparation

- **Work flow of mapreduce system**
	1. There will be one coordinator (master) and multiple workers (M map-workers and R reduce-workers). The coordinator picks idle workers and assigns each one a map task or a reduce task.
	2. A worker who is assigned a map task reads the contents of the corresponding input split. It parses key/value pairs out of the input data and passes each pair to the user-defined Map function. The intermediate key/value pairs produced by the Map function are buffered in memory.
	3. Periodically, the buffered pairs are written to local disk (temp files in the lab), partitioned into R regions (*with file name of mr-X-Y, which X is the map task number and Y is the reduce task number*) by the partitioning function. The locations of these buffered pairs on the local disk are passed back to the master, who is responsible for forwarding these locations to the reduce workers.
	4. When a reduce-worker is notified (*in the lab we notify the reduce-workers after all map-workers are done*) by the master about these locations, it uses *RPC* to read the buffered data from the local disks of the map-workers. When a reduce-worker has read all intermediate data, it sorts it by the intermediate keys so that all occurrences of the same key are grouped together. The sorting is needed because typically many different keys map to the same reduce task. If the amount of intermediate data is too large to fit in memory, an external sort is used.
	5. The reduce worker iterates over the sorted intermediate data and for each unique intermediate key encountered, it passes the key and the corresponding set of intermediate values to the user’s Reduce function. The output of the Reduce function is appended to a final output file for this reduce partition (*with the file name of mr-out-Y*).
	6. When all map tasks and reduce tasks have been completed, the master wakes up the user program. At this point, the MapReduce call in the user program returns back to the user code.
	![[mapreduce-workflow.png|820]]

> [!Hint] Hints
> 1. Only modify mr/worker.go, mr/coordinator.go and mr/rpc.go.
> 2. Start by modify mr/worker.go's Worker() to send an RPC to the coordinator asking for a task. Then modify the coordinator to respond with the file name of a to-do map task. Then modify the worker to read that file and call the application Map function, as in mrsequential.go.
> 3. Run mrapps using `go build -bildmode=plugin ../mrapps/wc.go`, or run individual test using `make RUN="-run Wc" mr`.
> 4. Store intermediate key/value pairs using Go's `encoding/json`.
> 5. Use `ihash(key)` in worker.go to pick the reduce task for a given key.
> 6. The coordinator should support concurrent RPC calls.
> 7. The time of considering a task's failure or stragglers (for Backup Tasks) is 10 seconds.
> 8. Use mrapps/crash.go to test recovery.
> 9. Use ioutil.TempFile or os.CreateTemp to create temp files and use os.Rename to rename it atomically.
> 10. Read the [rpc package - net/rpc - Go Packages](https://pkg.go.dev/net/rpc) for rpc usage. And [Standard library - Go Packages](https://pkg.go.dev/std) to find useful libs.

# Working

As the hints say, I start at the Worker() function in mr/worker.go. I first structured the Worker().
```go
// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	args := WorkerArgs{Status: Wait, IsFirstCall: true}
	// Waiting for the coordinator to assign tasks
	for {
		reply := WorkerReply{}
		ok := call("Coordinator.Assign", &args, &reply)
		if reply.Status == Wait {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		if !ok {
			log.Fatalf("RPC call failure")
		} else {
			switch reply.Status {
			case MapTask:
				//TODO
			case ReduceTask:
				//TODO
			case Exit:
				os.Exit(0)
			}
		}
	}
}
```
Then I designed the WorkerArgs() and WorkerReply() in mr/rpc.go.
```go
type Status int

const (
	MapTask Status = iota
	ReduceTask
	Wait
	MapDone
	ReduceDone
	Exit
)

// Worker RPC args
type WorkerArgs struct {
	Status         Status
	IsFirstCall    bool
	InterFileNames []string
	WorkerId       int
}

// Worker RPC reply
type WorkerReply struct {
	Status   Status
	WorkerId int
	TaskId   int
	Files    []string
	NReduce  int
}
```
Here the WorkerId is assigned when a worker first do the remote procedure call. The TaskId is assigned when the worker was assigned to a map or reduce job.

> [!note] Note
> My first design was not like this. At that time I didn't take the fault tolerance into consideration. So I only define the TaskId.

Then here is the Assign() function in mr/coordinator.go. 
```go
func (c *Coordinator) Assign(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.IsFirstCall {
		//TODO
	}
	switch args.status {
	case Wait:
		//TODO
	case MapDone:
		//TODO
	case ReduceDone:
		//TODO
	default:
		return errors.New("invalid task status")
	}
	return nil
}
```
The basic structure of Mapreduce is defined now, so I continue my work in the MapTask case in Worker()
```go
func Worker(sockname string, mapf func(string, string) []KeyValue,reducef func(string, []string) string) {
...
	case MapTask:
		//*debug
		// fmt.Printf("Worker %v start map task%v\n", reply.WorkerId, reply.TaskId)
		// Iterate all files
		kvs := []KeyValue{}
		for _, filename := range files {
			content, err := os.ReadFile(filename)
			if err != nil {
				log.Fatalf("Read file fail: %v", filename)
				args = WorkerArgs{Status: Wait}
				continue
			}
			kvs = append(kvs, mapf(filename, string(content))...)
		}
		// Generate temporary file for reduce worker
		interFileNames := writeIntermediate(kvs, reply.TaskId)
		args = WorkerArgs{Status: MapDone, InterFileNames: interFileNames, WorkerId: reply.WorkerId}
		//*debug
		// fmt.Printf("Map task %v done\n", reply.TaskId)
...
}
```
The work is very simple, read from the files assigned by coordinator (in this lab only 1 file per worker. However, I intense to make my design work in more common case). Then send the file content to mapf(), collecting the kvs, and write the kvs to intermediate files. After finish work, update the PRC args.
```go
func writeIntermediate(kvs []KeyValue, taskId int) []string {
	// Write all kvs to file to avoid open the file repeatedly
	//*debug
	// fmt.Println("Get in writeIntermediate")
	fileToContent := make(map[string]([]KeyValue))
	for _, kv := range kvs {
		reducerId := ihash(kv.Key) % nReduce
		filename := fmt.Sprintf("mr-%v-%v", taskId, reducerId)
		fileToContent[filename] = append(fileToContent[filename], kv)
	}
	//*debug
	// time.Sleep(time.Second)
	// fmt.Println("Finish fileToContent map")
	interFileNames := []string{}
	for filename, kvs := range fileToContent {
		interFileNames = append(interFileNames, filename)
		fTemp, err := os.CreateTemp("", "mr-tmp-*")
		tempPath := fTemp.Name()
		if err != nil {
			log.Fatal(err, "in temp file create")
		}
		enc := json.NewEncoder(fTemp)
		if err := enc.Encode(kvs); err != nil {
			log.Fatalf(err.Error())
		}
		fTemp.Close()
		err = os.Rename(tempPath, fmt.Sprintf("/tmp/%v", filename))
		if err != nil {
			log.Fatal(err)
		}
	}
	//*debug
	// fmt.Println("Finish create and write to temp file")
	return interFileNames
}
```
This is the writeIntermeadiate() function. I first iterate all kv in kvs, calculate the file name of witch the reducer should process later. Then store it in memory with a map. After that, I create all the temp files and store the kvs in corresponding files.

> [!note] Note
> The reason why I first store the kv and corresponding filename instead of write it immediately is to avoid frequent IO and complex logic

Then I go into the mr/coordinator. Finish the part of when should the Wait worker be assigned a map job. First I modify the Coordinator struct to record some necessary messages.
```go
type Coordinator struct {
	mu                sync.Mutex
	workerId          int // Fault tolerance
	allMapDone        bool // MapTask assign
	allReduceDone     bool // When to exit all workers
	interFileCnt      int // MapDone
	nReduce           int
	aliveWorker       int // Fault tolerance and when to exit
	taskId            int // Star from 0 in both map and reduce stage
	mapperCnt         int // MapTask
	reducerCnt        int // ReduceTask
	inputFile         []string
	intermediateFiles map[int][]string // MapDone and ReduceTask assign
	workerList        []WorkerEntry // Fault tolerance
	failTaskFiles     []Task // Fault tollerance
}
```
Not all the fields in Coordinator is used now. I mark the part of which they are really used. Then the assign part is as follows.
```go
...
case Wait:
	if !c.allMapDone {
		if len(c.inputFile) > 0 {
			// Have files to assign
			//*debug
			// fmt.Printf("Assign a map task with taskId=%v\n", c.taskId)
			expireTime := time.Now().Add(EXPIRE_TIME)
			worker = WorkerEntry{MapTask, worker.workerId, []string{c.inputFile[0]}, expireTime, c.taskId}
			c.replyAndSetWorkerList(&worker, reply)
			c.inputFile = c.inputFile[1:]
			c.mapperCnt++
			c.taskId++
			//*debug
			// fmt.Println(reply)
		} else {
			// No files to assign
			//*debug
			// fmt.Println("No file to map, just wait")
			expireTime := time.Now().Add(EXPIRE_TIME)
			worker.deadLine = expireTime
			c.replyAndSetWorkerList(&worker, reply)
		}
...
```
The expireTime and WorkerEntry will be discussed in the fault tolerance part. What the initial design do is just to assign a taskId and inputFile to worker. Then delete the corresponding inputFiles and increase the mapperCnt and taskId. If there no file to map, just wait. What to do next is deal with the finished job of mapper.
```go
...
case Mapdone:
	//*debug
	// fmt.Println("Maptask done, store inter file location")
	c.mapperCnt--
	c.interFileCnt += len(args.InterFileNames)
	for _, filename := range args.InterFileNames {
		parts := strings.Split(filename, "-")
		reducerId, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			log.Fatal(err, "in MapDone")
		}
		c.intermediateFiles[reducerId] = append(c.intermediateFiles[reducerId], filename)
	}
	//*debug
	// fmt.Println("Inter file stored")
	// All mappers done
	// This is the last mapper and all files are handled
	if c.mapperCnt == 0 && len(c.inputFile) == 0 && len(c.failTaskFiles) == 0 {
		//*debug
		// fmt.Println("All map work done")
		c.allMapDone = true
		c.taskId = 0
	}
	expireTime := time.Now().Add(EXPIRE_TIME)
	worker = WorkerEntry{Wait, worker.workerId, nil, expireTime, -1}
	c.replyAndSetWorkerList(&worker, reply)
...
```
First the mapperCnt-- and increase the interFileCnt. Then I collect the filename and store it into intermediaFiles with the reducerId mapping to it. This will be useful when assign the reduce job. Then I check if all map task done and send reply.
The map part comes to a conclusion. Then I do the reduce part also starting from Worker().
```go
case ReduceTask:
	//*debug
	// fmt.Printf("Worker %v start reduce task%v\n", reply.WorkerId, reply.TaskId)
	interFileNames := reply.Files
	reducerId := reply.TaskId
	kvs := readFromInterFiles(interFileNames)
	//*debug
	//fmt.Println(kvs)
	sort.Sort(KVList(kvs))
	keyToAllValues := make(map[string][]string)
	for _, kv := range kvs {
		keyToAllValues[kv.Key] = append(keyToAllValues[kv.Key], kv.Value)
	}
	//*debug
	// fmt.Println(keyToAllValues)
	oFile, err := os.Create(fmt.Sprintf("mr-out-%v", reducerId))
	if err != nil {
		log.Fatalf("Create file %v fail\n", oFile)
	}
	for k, vs := range keyToAllValues {
		res := reducef(k, vs)
		fmt.Fprintf(oFile, "%v %v\n", k, res)
	}
	args = WorkerArgs{Status: ReduceDone, WorkerId: reply.WorkerId}
	//*debug
	// fmt.Printf("Reduce task %v done\n", reply.TaskId)
```
First I read files and reducerId from reply, and parse kvs from the interFileNames. Then I sort the kvs and combine all the values with the same key and send it to reducef to get the res to write in mr-out-reducerId. The readFromInterFiles and sort implement is as follows.
```go
type KVList []KeyValue

func (kv KVList) Len() int           { return len(kv) }
func (kv KVList) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv KVList) Less(i, j int) bool { return kv[i].Key < kv[i].Key }

func readFromInterFiles(interFileNames []string) (allkvs []KeyValue) {
	for _, filename := range interFileNames {
		f, err := os.Open(fmt.Sprintf("/tmp/%v", filename))
		if err != nil {
			log.Fatal(err, "in readFromInterFiles")
		}
		var kvs []KeyValue
		dec := json.NewDecoder(f)
		if err = dec.Decode(&kvs); err != nil {
			log.Fatal(err, "in decode")
		}
		allkvs = append(allkvs, kvs...)
	}
	return
}
```
Very simple use of encoding/json and sort.Sort()
Then goes back to the mr/coordinator.go. The code is as follows.
```go
case Wait:
	if c.interFileCnt > 0 {
		//*debug
		// fmt.Printf("Assign a reduce task with taskId=%v\n", c.taskId)
		interFileNames := c.intermediateFiles[c.taskId]
		expireTime := time.Now().Add(EXPIRE_TIME)
		worker = WorkerEntry{ReduceTask, worker.workerId, interFileNames, expireTime, c.taskId}
		c.replyAndSetWorkerList(&worker, reply)
		c.interFileCnt -= len(c.intermediateFiles[c.taskId])
		c.taskId++
		c.reducerCnt++
```
