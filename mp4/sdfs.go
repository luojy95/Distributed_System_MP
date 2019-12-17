package main
import(
	"gopkg.in/yaml.v2"
	"net/http"
	"net/rpc"
	"sort"
	"time"
	"fmt"
	"os"
	"net"
	"log"
	"sync"
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"math/rand"
	"hash/fnv"
	"io/ioutil"
	"os/exec"
	"bufio"
	// "sync"
)
const deliminator = "qwekjrhkjsdbfakjsdfkajs"
const version_delim = "lasdflahsdlfjasdlkfhwef"
const version_seg = "# =================================================================== #"
const NUM_REPLICA = 4
const LOCAL = "local_dir"
const SDFS = "sdfs_dir"
const LOCAL_FIND = "cd ./local_dir && "
const SDFS_FIND = "cd ./sdfs_dir && "
const SORT = " | sort -z"
const BATCH_SIZE = 4096
const LIMIT = 20 

// @TODO
/**
	Finalize the input format for master
**/

type T struct{
	Funk int
	Op int
	Tar string
	File string
}

type Mission struct {
	Layer int
	Tasks []T
}

type Job struct {
	Parent_num int
	Child_num int
	Parent []int
	Child []int
	Task T
	File string
}

func create_task_yml(child []int, parent []int, function int, op int, tar string, file string){
	// Master create yaml file which is sent to workers
	f, _ := os.OpenFile("task.yml", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "parent_num: %d\n", len(parent))
	fmt.Fprintf(w, "child_num: %d\nparent:\n", len(child))
	for i:=0; i < len(parent); i++{
		fmt.Fprintf(w, "  - %d\n", parent[i])
	}
	fmt.Fprintf(w, "child:\n")
	for i:=0; i < len(child); i++{
		fmt.Fprintf(w, "  - %d\n", child[i])
	}
	fmt.Fprintf(w, "task:\n")
	fmt.Fprintf(w, "  funk: %d\n", function)
	fmt.Fprintf(w, "  op: %d\n", op)
	fmt.Fprintf(w, "  tar: %s\n", tar)
	fmt.Fprintf(w, "  file: %s", file)
	w.Flush()
}
		
func create_task_assignment(Alive_list []int, num_inner_layer int)(task_list []([]int)){
	// task_list start from spout layer, until client, try to allocate all nodes as workers.
    num_worker := len(Alive_list) - 1
	// var task_list []([]int)
	// task_list = []([]int){[]int{Alive_list[0]}} 
	task_list = []([]int){[]int{0}} 
	cur_inner_layer := num_inner_layer
	idx := 0
	for i := 0; i < cur_inner_layer; i++{
		num := 0
		if num_worker/num_inner_layer * num_inner_layer == num_worker{
			num = num_worker/num_inner_layer
		}else{
			num = num_worker/num_inner_layer + 1
		}
		if num == 0{
			fmt.Println("Not enough node!")
			return []([]int){}
		}
		var temp []int
		for j := 0;  j < num; j++{
			temp = append(temp, Alive_list[idx])
			idx ++
		}
		// fmt.Printf("num: %#v\n", num)
		// fmt.Printf("num_worker: %#v\n", num_worker)
		// fmt.Printf("num_inner_layer: %#v\n", num_inner_layer)
		num_worker -= num
		num_inner_layer -= 1
		task_list = append(task_list, temp)
	}
	//append sink list
	task_list = append(task_list, []int{Alive_list[len(Alive_list) - 1]})
	// fmt.Printf("Value: %#v\n", task_list)
	return
}

func (self *Daemon)send_assignment(worker_id int, child []int, m Mission, layer_id int){
	//Helper function of parse_yaml, to send job to each worker
	// fmt.Println("Layer id is: ", layer_id)
	// fmt.Println("Tar is: ", m.Tasks[layer_id].Tar)
	// fmt.Println("Func is: ", m.Tasks[layer_id].Funk)
	ip := parse_string(self.MemberList[worker_id], 1)
	create_task_yml(child, []int{}, m.Tasks[layer_id].Funk, m.Tasks[layer_id].Op, m.Tasks[layer_id].Tar, m.Tasks[layer_id].File)
    task, err := ioutil.ReadFile("task.yml")
    // fmt.Println()
    if err != nil {
        panic(err)
    }
    task_string := string(task[:])
    // action := "updateTopology"
    // mesg := action + deliminator + task_string
    newMap := make(map[int]string)
	newMap[-1] = "updateTopology"
	newMap[-2] = task_string
	// fmt.Println("first: ",task_string)
	ip_to_send := change_port_to_6060(ip)
    self.send_tcp(ip_to_send, newMap, worker_id)
}

func (self *Daemon)parse_yaml(path string){
	// Start parsing the yaml input file, only for master (nimbus)
	// send Jobs to all workers

	// fmt.Println("Parsing yaml!")
	var m Mission
	source, err := ioutil.ReadFile(path)
    if err != nil {
        fmt.Println("Read yml file failed2!")
    }
    err = yaml.Unmarshal(source, &m)
    if err != nil {
        fmt.Println("Unmarshal failed!")
    }

    // Assign those tasks based on the alive machine number
    var Alive_list []int
	for _, v := range(self.MemberList) {
		id,_ := strconv.Atoi(parse_string(v, 0))
		status := parse_string(v, 2)
		if status == "1" && id != 0 && id != 1{
			Alive_list = append(Alive_list, id)
		}		
	}
	sort.Ints(Alive_list)
	// fmt.Println("m.Layerm.Layerm.Layerm.Layer ", m.Layer)
	// fmt.Println(Alive_list)

	// Change client to the last location
	new_list := []int{}
	if if_in_list(Alive_list, self.clientID) >= 0{
		loc := if_in_list(Alive_list, self.clientID)
		// new_list := []int{}
		for i := 0; i < len(Alive_list); i++{
			if i != loc{
				new_list = append(new_list, Alive_list[i])
			}
		}
		new_list = append(new_list, Alive_list[loc])
	}else{
		fmt.Println("Client has died!")
		return
	}

	task_list := create_task_assignment(new_list, m.Layer)

	// fmt.Println("task_list: ",task_list)
	child := []int{}
	curr := []int{}
	for i := 0; i < len(task_list); i++{
		if i == len(task_list) - 1{
			// parent = []int{}
			child = []int{}
			curr = task_list[i]
			for j:=0; j < len(curr);j++{
				self.send_assignment(curr[j], child, m, i)
			}
		}else{
			// parent = task_list[i]
			child = task_list[i + 1]
			curr = task_list[i]
			for j:=0; j < len(curr);j++{
				self.send_assignment(curr[j], child, m, i)
			}
		}
	}
}

func (self *Daemon)restart_parse_yaml(){
    newMap := make(map[int]string)
	newMap[-1] = "restart_parse_yaml"
	newMap[-2] = ""
	ip_addr := parse_string(self.MemberList[self.clientID], 1)
	ip_to_send := change_port_to_6060(ip_addr)
    self.send_tcp(ip_to_send, newMap, self.clientID)
}


func (self *Daemon)parse_yaml_stream(s string){
	// Start parsing the yaml input file, only for master (nimbus)
	// send Jobs to all workers

	// fmt.Println("Parsing yaml!")
	// var m Mission
	// source, err := ioutil.ReadFile(path)
 //    if err != nil {
 //        fmt.Println("Read yml file faile!")
 //    }
 //    err = yaml.Unmarshal(source, &m)
 //    if err != nil {
 //        fmt.Println("Unmarshal failed!")
 //    }
	source := []byte(s)
	var m Mission
    err := yaml.Unmarshal(source, &m)
    if err != nil {
        fmt.Println("Unmarshal failed!")
    }
    // Assign those tasks based on the alive machine number
    var Alive_list []int
	for _, v := range(self.MemberList) {
		id,_ := strconv.Atoi(parse_string(v, 0))
		status := parse_string(v, 2)
		if status == "1" && id != 0 && id != 1{
			Alive_list = append(Alive_list, id)
		}		
	}
	sort.Ints(Alive_list)
	// fmt.Println("m.Layerm.Layerm.Layerm.Layer ", m.Layer)
	// fmt.Println(Alive_list)

	// Change client to the last location
	new_list := []int{}
	if if_in_list(Alive_list, self.clientID) >= 0{
		loc := if_in_list(Alive_list, self.clientID)
		// new_list := []int{}
		for i := 0; i < len(Alive_list); i++{
			if i != loc{
				new_list = append(new_list, Alive_list[i])
			}
		}
		new_list = append(new_list, Alive_list[loc])
	}else{
		fmt.Println("Client has died!")
		return
	}

	task_list := create_task_assignment(new_list, m.Layer)

	// fmt.Println("task_list: ",task_list)
	child := []int{}
	curr := []int{}
	for i := 0; i < len(task_list); i++{
		if i == len(task_list) - 1{
			// parent = []int{}
			child = []int{}
			curr = task_list[i]
			for j:=0; j < len(curr);j++{
				self.send_assignment(curr[j], child, m, i)
			}
		}else{
			// parent = task_list[i]
			child = task_list[i + 1]
			curr = task_list[i]
			for j:=0; j < len(curr);j++{
				// fmt.Println("curr: ",curr)
				// fmt.Println("child: ",child)
				// fmt.Println("m: ",m)
				// fmt.Println("i: ",i)
				self.send_assignment(curr[j], child, m, i)
			}
		}
	}
}

// func (self *Daemon) read_storm()(err error){
// 	fmt.Println("Starting reading storm")
// 	for {
// 		m := make(map[int]string)
// 		Listener := self.Storm_listener
// 		inputBytes := make([]byte, 4096)
// 	  	length, _, err := Listener.ReadFromUDP(inputBytes)
// 	  	if err != nil{
// 	  		return err
// 	  	}
// 	  	buffer := bytes.NewBuffer(inputBytes[:length])
// 	  	decoder := gob.NewDecoder(buffer)
// 	  	decoder.Decode(&m)
// 	  	fmt.Println("receive: ", m)
// 	  	self.parse_storm(m)
//   	}
//   	return nil
// }

func (self *Daemon) read_storm()(err error){
	// fmt.Println("Starting reading storm")
	// fmt.Println("accept 123")
	for {
		conn, err := self.Storm_listener.Accept()
		if err != nil{
	  		fmt.Println("Error here")
	  		panic(err)
	  	}
		// m := make(map[int]string)
		// inputBytes := make([]byte, 4096)
		// n, _ := conn.Read(inputBytes)
		// // fmt.Println("string(inputBytes) and n", string(inputBytes), n)

	 //  	buffer := bytes.NewBuffer(inputBytes[:n])
	 //  	decoder := gob.NewDecoder(buffer)
	 //  	decoder.Decode(&m)
	 //  	// fmt.Println("receive: ", m)
	 //  	self.parse_storm(m)
		go self.handleConn(conn)
  	}
  	return nil
}

func (self *Daemon) handleConn(conn net.Conn) {
    defer conn.Close()
	m := make(map[int]string)
	inputBytes := make([]byte, 4096)
	n, _ := conn.Read(inputBytes)
	// fmt.Println("string(inputBytes) and n", string(inputBytes), n)

  	buffer := bytes.NewBuffer(inputBytes[:n])
  	decoder := gob.NewDecoder(buffer)
  	decoder.Decode(&m)
  	// fmt.Println("receive: ", m)
  	self.parse_storm(m)
}

func (self *Daemon) parse_storm(m map[int]string){
	// fmt.Println("Starting parse storm")
	if (len(m) != 2){
		fmt.Println("Not valid storm input")
	}
	if m[-1] == "updateTopology"{
		// fmt.Println("start updating topology")
		self.initial_task(m[-2])
	}else if m[-1] == "process"{
		// fmt.Println("start processing processing")
		self.add_task(m[-2])
	}else if strings.Contains(m[-1], "master_parse_yaml"){
		ID := strings.Split(m[-1], ":")[1]
		// fmt.Println("client id: ", ID)
		self.clientID,_ = strconv.Atoi(ID)
		self.parse_yaml_stream(m[-2])
	}else if m[-1] == "restart_parse_yaml"{
		time.Sleep(200 * time.Millisecond)
		self.resend_yaml_file()
		time.Sleep(200 * time.Millisecond)
	}
}

func(self* Daemon)resend_yaml_file(){
	// fmt.Println("Client resend yaml")
    if (self.ID == self.masterID) {
		self.parse_yaml(self.task_file)
	}else{
	    newMap := make(map[int]string)
		newMap[-1] = "master_parse_yaml:" + strconv.Itoa(self.ID)

		// fmt.Println("Parsing yaml!")
		// fmt.Println("self yaml file:", self.task_file)
		source, err := ioutil.ReadFile(self.task_file)
	    if err != nil {
	        fmt.Println("Read yml file failed!")
	    }
		task := string(source[:])	
		newMap[-2] = task
		ip_addr := parse_string(self.MemberList[self.masterID], 1)
		ip_to_send := change_port_to_6060(ip_addr)
	    self.send_tcp(ip_to_send, newMap, self.masterID)
	}
}
// else if m[-1] == "master_parse_yaml" {
// 		self.parse_yaml_stream(m[-2])
// 	}

func new_split(tuple string)(ret []string) {
// tuple := "asaf, asfe, \"asdfa,sdf\""
	 flag := 0
	 ret = strings.FieldsFunc(tuple, func(r rune) bool {
	  switch r {
	  case ',':
	   if flag == 0{
	    return true
	   }
	   return false
	  case '"':
	   flag = 1 - flag
	   return true
	  }
	  return false
	 })
	 return
}

func read_db(path string)(db [][]string){
	file, err := os.Open(path)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
	flag := 0
	for scanner.Scan() {
		tup := scanner.Text()
		tup_list := strings.Split(tup, " ")
		if (flag == 0) {
			// fmt.Println("First tup_list LENTH", len(tup_list))
			for j:=0; j<len(tup_list); j++{
				db = append(db, []string{tup_list[j]})
			}
			flag = 1
		}else{
			// fmt.Println("len(tup_list", len(tup_list))
			for j:=0; j<len(tup_list); j++{
				if (j > len(db)-1) {
					// fmt.Println("exceed limit", tup_list)
					continue
				}
				db[j] = append(db[j], tup_list[j])
			}
		}
	}
	return
}

func (self *Daemon)initial_task(source string){

	// fmt.Println("Start initial task")
	// fmt.Println("second: ",source)
	s := []byte(source)
	var j Job
	err := yaml.Unmarshal(s, &j)
	if err != nil {
        panic(err)
    }
    self.count_jobs = 0
    // fmt.Println("Finish initial task1")
    // fmt.Println("JOB!!!: ", j)
    self.mutex_task.Lock()
    self.task_queue = []string{}
    self.outputMap = make(map[string]int)
    self.outputString = ""
	self.funk = j.Task.Funk
	self.op = j.Task.Op
	self.tar = j.Task.Tar
	self.num_child = j.Child_num
	self.child = j.Child
	self.robin = 0
	self.LOCAL_FILE = j.Task.File
	if (self.funk == 5) {
		self.static_db = read_db(self.LOCAL_FILE)
	}
	self.mutex_task.Unlock()
	// fmt.Println("self.LOCAL_FILE:", j.Task.File)
	done4 := make(chan bool, 1)
	// fmt.Println("Finish initial task2")
	go self.do_task(done4)
	// fmt.Println("Finish initial task3")
}

func (self *Daemon)add_task(task string){
	// fmt.Println("ADDD!!!!chiningmeng")
	self.mutex_task.Lock()
	self.task_queue = append(self.task_queue, task)
	self.mutex_task.Unlock()
}

func (self *Daemon)filter(tuple string)(ret int){
	// fmt.Println("Start filter")
	ret = 0
	if strings.Contains(tuple, self.tar){
		i := self.robin % len(self.child)
		// self.mutex_task.Lock()
		self.robin = (self.robin + 1) % len(self.child)
		// self.mutex_task.Unlock()
		ip := parse_string(self.MemberList[self.child[i]], 1)
		ip_to_send := change_port_to_6060(ip)
	    newMap := make(map[int]string)
		newMap[-1] = "process"
		newMap[-2] = tuple	

		ret = self.send_tcp(ip_to_send, newMap, self.child[i])
		// fmt.Println("filter emit: ",ip_to_send, newMap)	
	}
	return
}

func (self *Daemon)join_db(tuple string){
	// fmt.Println("Start filter")
	tuple_idx, _ := strconv.Atoi(strings.Split(self.tar, "_")[0])
	db_idx, _ := strconv.Atoi(strings.Split(self.tar, "_")[1])
	tuple_list := new_split(tuple)

	if (if_in_strlist(self.static_db[db_idx], tuple_list[tuple_idx]) >= 0){
		i := self.robin % len(self.child)
		self.mutex_task.Lock()
		self.robin = (self.robin + 1) % len(self.child)
		self.mutex_task.Unlock()
		ip := parse_string(self.MemberList[self.child[i]], 1)
		ip_to_send := change_port_to_6060(ip)
	    newMap := make(map[int]string)
		newMap[-1] = "process"
		newMap[-2] = tuple	
		self.send_tcp(ip_to_send, newMap, self.child[i])
		// fmt.Println("join emit: ",ip_to_send, newMap)	
	}
}

func (self *Daemon)stringAggregate(tuple string){
	// fmt.Println("Start agg")
	op := self.op
	if op == 0{
		self.outputString += tuple + "\n"
	}else if op == 1{
		self.outputString += (strings.ToUpper(tuple) + "\n")
	}else if op == 2{
		self.outputString += (strings.ToLower(tuple) + "\n")
	}
}

func (self *Daemon)mapAggregate(tuple string){
	ret := strings.FieldsFunc(tuple, func(r rune) bool {
		switch r {
		case ',', '|', ' ':
			return true
		}
		return false
	})
    for j:=0; j<len(ret); j++{
		if _, ok := self.outputMap[ret[j]]; !ok { //if not in outputMap
		    self.outputMap[ret[j]] = 1
		} else { //if in outputMap, increment
			self.outputMap[ret[j]]++;
		}
    }
}

func (self *Daemon)couting(tuple string)(ret int){
	// fmt.Println("Start filter")
	ret = 0
	if strings.Contains(tuple, self.tar){
		i := self.robin % len(self.child)
		// self.mutex_task.Lock()
		self.robin = (self.robin + 1) % len(self.child)
		// self.mutex_task.Unlock()
		ip := parse_string(self.MemberList[self.child[i]], 1)
		ip_to_send := change_port_to_6060(ip)
	    newMap := make(map[int]string)
		newMap[-1] = "process"
		newMap[-2] = tuple	

		ret = self.send_tcp(ip_to_send, newMap, self.child[i])
		// fmt.Println("filter emit: ",ip_to_send, newMap)	
	}
	return
}

// func (self *Daemon)split_emit(tuple string){
// 	// ret := strings.Split(tuple, " ")

// 	ret := strings.FieldsFunc(tuple, func(r rune) bool {
// 		switch r {
// 		case ',', '|', ' ':
// 			return true
// 		}
// 		return false
// 	})
//     for j:=0; j<len(ret); j++{
//         i := self.robin % len(self.child)
//         self.mutex_task.Lock()
// 		self.robin = (self.robin + 1) % len(self.child)
// 		self.mutex_task.Unlock()
// 		ip := parse_string(self.MemberList[self.child[i]], 1)
// 		ip_to_send := change_port_to_6060(ip)
// 	    newMap := make(map[int]string)
// 		newMap[-1] = "process"
// 		newMap[-2] = ret[j]	

// 		self.send_tcp(ip_to_send, newMap, self.child[i])
// 		// fmt.Println("split emit: ",ip_to_send, newMap)	
//     }
// }

func (self *Daemon)do_task(done4 chan bool){
	// fmt.Println("Do task???")
	// fmt.Println("funk: ", self.funk)
	if self.funk == 0{
		// fmt.Println("LOCAL_FILE: !!!!!!!!!!!!1", self.LOCAL_FILE)
		self.create_stream(self.LOCAL_FILE)
	}
	// }else{
		// counter := 0
		for{
			// fmt.Println("Do task!!")
			self.mutex_task.Lock()
			if len(self.task_queue) > 0{
				if(self.count_jobs % 50 == 0){
					fmt.Println("Count: ", self.count_jobs + 1)
				}
				// self.count_jobs ++
				task := self.task_queue[0]
				self.task_queue = self.task_queue[1:]
				self.mutex_task.Unlock()

				
				self.mutex_task.Lock()
				if self.funk == 0{
					i := self.robin % len(self.child)
					self.robin = (self.robin + 1) % len(self.child)
					ip := parse_string(self.MemberList[self.child[i]], 1)
					ip_to_send := change_port_to_6060(ip)
				    newMap := make(map[int]string)
					newMap[-1] = "process"
					newMap[-2] = task
					success := self.send_tcp(ip_to_send, newMap, self.child[i])	
					if (success != 0) {
						// fmt.Println("PUSH BACK CREATE STREAM Again !!!!!!!!!")
						self.task_queue = append(self.task_queue, newMap[-2])
					} else {
						self.count_jobs++
					}
				}

				success_send := 0
				if self.funk == 1{
					// counter ++
					success_send = self.filter(task)
				}
				if self.funk == 2{
					// counter ++
					self.stringAggregate(task)
				}
				if self.funk == 3{//filter and couting
					success_send = self.couting(task)
					// self.split_emit(task)
				}
				if self.funk == 4{
					self.mapAggregate(task)
				}
				if self.funk == 5{
					self.join_db(task)
				}
				self.mutex_task.Unlock()

				self.mutex_task.Lock()
				if (success_send != 0){
					// fmt.Println("PUSH BACK CREATE task_queue !!!!!!!!!")
					self.task_queue = append(self.task_queue, task)
				} else {
					if (self.funk !=0){
						self.count_jobs ++
					}
				}
				self.mutex_task.Unlock()


				// fmt.Println("Really do task")
				// if (counter > 0) {
				// 	done4 <- true
				// }
			}else{
				self.mutex_task.Unlock()
			}

			// self.mutex_task.Unlock()
			// if {
				// done4 <- true
			// }
		}
		
	// }
}

// func (self *Daemon)do_task(done4 chan bool){
// 	// fmt.Println("Do task???")
// 	// fmt.Println("funk: ", self.funk)
// 	if self.funk == 0{
// 		// fmt.Println("LOCAL_FILE: !!!!!!!!!!!!1", self.LOCAL_FILE)
// 		self.create_stream(self.LOCAL_FILE)
// 	}else{
// 		// counter := 0
// 		for{
// 			// fmt.Println("Do task!!")
// 			self.mutex_task.Lock()
// 			if len(self.task_queue) > 0{
// 				if(self.count_jobs % 50 == 0){
// 					fmt.Println("Count: ", self.count_jobs)
// 				}
// 				self.count_jobs ++
// 				task := self.task_queue[0]
// 				self.task_queue = self.task_queue[1:]
// 				self.mutex_task.Unlock()

// 				self.mutex_task.Lock()
// 				if self.funk == 1{
// 					// counter ++
// 					self.filter(task)
// 				}
// 				if self.funk == 2{
// 					// counter ++
// 					self.stringAggregate(task)
// 				}
// 				if self.funk == 3{
// 					self.split_emit(task)
// 				}
// 				if self.funk == 4{
// 					self.mapAggregate(task)
// 				}
// 				if self.funk == 5{
// 					self.join_db(task)
// 				}
// 				self.mutex_task.Unlock()

// 				// fmt.Println("Really do task")
// 				// if (counter > 0) {
// 				// 	done4 <- true
// 				// }
// 			}else{
// 				self.mutex_task.Unlock()
// 			}

// 			// self.mutex_task.Unlock()
// 			// if {
// 				// done4 <- true
// 			// }
// 		}
		
// 	}
// }

func (self *Daemon)create_stream(path string){

	file, err := os.Open(path)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    // fmt.Println("self.child", self.child)
    for scanner.Scan() {
		i := self.robin % len(self.child)
		self.mutex_task.Lock()
		self.robin = (self.robin + 1) % len(self.child)
		self.mutex_task.Unlock()
		ip := parse_string(self.MemberList[self.child[i]], 1)
		ip_to_send := change_port_to_6060(ip)
	    newMap := make(map[int]string)
		newMap[-1] = "process"
		newMap[-2] = scanner.Text()	

		success := self.send_tcp(ip_to_send, newMap, self.child[i])	
		
		if (success != 0) {
			// fmt.Println("PUSH BACK CREATE STREAM !!!!!!!!!")
			self.mutex_task.Lock()
			self.task_queue = append(self.task_queue, newMap[-2])
			self.mutex_task.Unlock()
		} else {
			self.mutex_task.Lock()
			self.count_jobs++
			self.mutex_task.Unlock()
		}
		
	    	// send(ip_to_send, scanner.Text())
	    	// fmt.Println("emit process data", ip_to_send, i)
	}

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
    return 
}

func parse_string(s string, idx int)(ret string){
	ret = strings.Split(s, "###")[idx]
	return ret
}

func change_port(s string)(ret string){
	ret = strings.Split(s, ":")[0] + ":8080"
	return ret
}

func change_port_to_6060(s string)(ret string){
	ret = strings.Split(s, ":")[0] + ":6060"
	return ret
}

func getMyID()(perline int){
        // file, _ := os.Open("/home/jiayil5/local_info/info.txt")
		file, _ := os.Open("/home/zjiang30/local_info/info.txt")
        fmt.Fscanf(file, "%d\n", &perline)
	return
}

func hash(s string) uint32 {
        h := fnv.New32a()
        h.Write([]byte(s))
        return h.Sum32()
}

const (
	T_read = 1500
	limit = 4
	T_fail = 30000000000
	layout = "2018-10-07 18:18:11.99876407 -0500 CDT m=+0.002631354"
)

type Daemon struct {
	jitiao int
	count_jobs int
	masterIP []string
	task_file string
	clientID int
	static_db [][]string
	LOCAL_FILE string
	mutex sync.Mutex
	mutex_task sync.Mutex
	// mutex_task sync.Mutex
	outputMap map[string]int
	outputString string
	masterID int
	task_queue []string
	robin int
	funk int // Task function
	op int // Task operator
	tar string // Task target
	num_child int // # of child
	child []int // child list
	// LDHT []string
	GDHT map[string][]int // only for master
	Listener *net.UDPConn
	Storm_listener net.Listener
	// FileListener *net.
	addr string
	NeighborList []int
	MemberList map[int]string // id + address + status + time
	port int
	ID int
	role int //
	status int // 0 failed; 1 alive; 2 leave; 3 rejoin
	fail_counter map[int]int
	random_count int
	HistoryMemberList map[int]string 
	timestamp time.Time
	comparetime map[string]string //sdfs_name ---> "VM ID"+"timestamp"
	fileCounter int
	fileCounter_List []int
	// versionCounter int
	ver_array []int
	getVersionFileCounter int

}

// ============================================================================================//
// ============================================================================================//
type Arith int
func(self *Daemon)start_File_server(done3 chan bool){
    arith := new(Arith)
    rpc.Register(arith)
    rpc.HandleHTTP()
    my_ip:= get_ip()
    my_ip = strings.TrimSuffix(my_ip, "\n")
    // Create listener
    listener, err:= net.Listen("tcp", my_ip + ":8080")
    // Assert error
    if err != nil {
        fmt.Println("Can not create listener")
    }
    // Clean up listener when done
    defer listener.Close()
    go http.Serve(listener, nil)
    select{}
}

// Helper function
func Write_2_file(s string, path string, directory string)(e int){
        var b []byte
        b = []byte(s)
        e = 0
        // if (need_directory) {
        err := ioutil.WriteFile(directory + "/" + path, b, 0644)
        if err != nil {
        	e = 1;
        }
    return
}

// Assert Error
func assert(err error, output string){
        if err != nil {
                fmt.Println(output)
                log.Fatalln(err)
        }
}

func (t *Arith) Command_grep_get (command *string, reply *string) error{
	sdfs_name := parse_string(*command, 0)
	version := parse_string(*command, 1)
    var out bytes.Buffer
    var out_file bytes.Buffer
    find_cmd := SDFS_FIND + "find . -name \"" + sdfs_name + "_*_" + version + "_\"" + SORT
    file_cmd := exec.Command("/bin/bash","-c", find_cmd)
    file_cmd.Stdout = &out_file
    err := file_cmd.Run()
    ret := strings.Split(out_file.String(), "\n")
    if (len(ret) == 1){
    	// fmt.Println("The sdfs file is not available!")
    }else{
	    file_found := ret[len(ret) - 2]
	    cat_cmd := SDFS_FIND + "cat " + file_found
	    cmd := exec.Command("/bin/bash","-c",cat_cmd)
	    cmd.Stdout = &out
	    err = cmd.Run()
	    // *reply = out.String()
	    if err != nil {
	        //log.Fatalln(err)
	        fmt.Println("No matching result")
	    }else{
	    	(*reply) = strings.TrimPrefix(file_found, "./") + deliminator + out.String()
	    }
	}
    return nil
} 

func get_ip() string {
  var err error
  ls := exec.Command("hostname", "-i")
  // grep.Stdin, err = ls.StdoutPipe()
  out, err := ls.Output()
  if err != nil {//if there is no matching result
     fmt.Println("No matching result")
      return "Fail to fetch ip_address"
  }
  return string(out[:])
}



// ============================================================================================//
// ============================================================================================//

func (self *Daemon)get(sdfs_name string, local_name string, version string){ // 
	// send request to master asking for sdfs_name
	self.fileCounter = 0
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + local_name + "###" + sdfs_name + "###" + version
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "ask_for_file_get", Id_info, change_time(time.Now()))
	//send file update DHT info to master
	masterAddr := parse_string(self.MemberList[self.masterID], 1)
	send(masterAddr, ret);
}

func (self *Daemon) start_fetch_file_get(m map[int]string) { 
	//Master ask for file from 5 clients with replica of the original file
	Info := m[-2]
	// file_name := parse_string(Info, 4)
	// fmt.Println(Info)
	sdfsfile_name := parse_string(Info, 5)
	version := parse_string(Info, 6)
	requestor, _ := strconv.Atoi(parse_string(Info, 0)) //vm that request for file

	VM_ID_list := self.GDHT[sdfsfile_name]

	// VM_ID_list := []int{1, 2}
	VM_ID_list_string := ""
	for i:= 0; i < len(VM_ID_list); i++{
		VM_ID_list_string += (strconv.Itoa(VM_ID_list[i]) + "###")
	}
	ID_info := VM_ID_list_string + sdfsfile_name + "###" +  parse_string(Info, 4) + "###" + version// 1###2###3###asdf###word#0
	emptyMap := make(map[int]string)
	for k,v := range self.MemberList {
	  emptyMap[k] = v
	}
	ret := change_list_to_stream(emptyMap, "require_latest_file_get", ID_info, change_time(time.Now()))
	requestor_vm_IP := parse_string(self.MemberList[requestor], 1)
	if (len(VM_ID_list) == 0) {
		ret := change_list_to_stream(emptyMap, "print_sdfs_not_available", ID_info, change_time(time.Now()))
		send(requestor_vm_IP, ret)
	} else {
		send(requestor_vm_IP, ret)
	}
	
}

func (self *Daemon)send_latest_file_get(m map[int]string){
	// Each client send the required replica to the requestor
	Info := m[-2] // 1###2###3###asdf###word#0
	// fmt.Println("Info before replica num", Info)
	ID_array := strings.Split(Info, "###")
	sdfs_name := ID_array[len(ID_array) - 3]
	file_name := ID_array[len(ID_array) - 2]
	version := ID_array[len(ID_array) - 1]

	num_replica := len(ID_array) - 3 // 3
	// fmt.Println("replica num", num_replica)
	for i:=0; i < num_replica; i++{
		ID,_ := strconv.Atoi(ID_array[i])
		replica_IP_old := parse_string(self.MemberList[ID], 1)
		replica_IP := change_port(replica_IP_old) // change from 4040 to 8080 port
		// file_name := ID_array[len(ID_array) - 1]
		self.start_File_get(replica_IP, sdfs_name, file_name, version) // Ask file from servers
	}
}

func(self *Daemon)start_File_get(ip_address string, sdfs_name string, local_name string, version string){
    // fmt.Println("HERE2")
    // fmt.Println("ip_address ", ip_address, "self.ID ", self.ID)
    // fmt.Println("no problem, ", sdfs_name, local_name)
    conn, err := rpc.DialHTTP("tcp",ip_address)
    assert(err, "error to dial")
    defer conn.Close()
    // local_latest_name := self.LDHT[command][0]
    args := sdfs_name + "###" + version
    var reply string 

    err = conn.Call("Arith.Command_grep_get", &args, &reply)
    assert(err, "Error")
    // fmt.Println("reply ", reply)
	// delete_oldest_file(sdfs_name)
    // Write_2_file(reply, sdfs_name + "_" + change_time(time.Now()) + "_" + "0_")
    file_name := strings.Split(reply, deliminator)[0]
    file_content := strings.TrimPrefix(reply, file_name + deliminator)
    if (self.fileCounter >= 0 && (self.fileCounter < int(NUM_REPLICA/2) + 1) && (Write_2_file(file_content, file_name, LOCAL) == 0)){
    	self.fileCounter++;
    	// self.versionCounter--;
    	// fmt.Println("versionCounter 1             ", self.versionCounter)
    }
    if (self.fileCounter >= 0 && self.fileCounter >= int(NUM_REPLICA/2) + 1){
        // fmt.Println(file_name, " adfadsafds")
    	self.filter_latest_file(file_name, local_name)
    	self.fileCounter = -1
    	// self.getVersionFileCounter ++
    }
}

func(self *Daemon)filter_latest_file(file_name string, local_name string){
	var out_file bytes.Buffer
	version := strings.Split(file_name, "_")[2]
	sdfs_name_with_prefix := strings.Split(file_name, "_")[0]
	sdfs_name := strings.TrimPrefix(sdfs_name_with_prefix, "./")
	find_cmd := LOCAL_FIND + "find . -name \"" + sdfs_name + "_*_" + version + "_\"" + SORT
    file_cmd := exec.Command("/bin/bash","-c", find_cmd)
    file_cmd.Stdout = &out_file
    err := file_cmd.Run()
	if err != nil{
	    fmt.Println("File not found!")
	}
	ret := strings.Split(out_file.String(), "\n")
	for i :=0; i < len(ret) - 2; i++{
		delete_cmd := LOCAL_FIND + "rm " + ret[i]
		D_cmd := exec.Command("/bin/bash","-c", delete_cmd)
		D_cmd.Run()
	}
	rename_cmd := LOCAL_FIND + "mv " + ret[len(ret) - 2] + " " +local_name
	R_cmd := exec.Command("/bin/bash","-c", rename_cmd)
	R_cmd.Run()	
	// fmt.Println("Finish 1 iteration")
}

// ============================================================================================//


func (self *Daemon)get_v(sdfs_name string, local_name string, version string){ // 
	// send request to master asking for sdfs_name
	self.fileCounter = 0
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + local_name + "###" + sdfs_name + "###" + version
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "ask_for_file_get_v", Id_info, change_time(time.Now()))
	//send file update DHT info to master
	masterAddr := parse_string(self.MemberList[self.masterID], 1)
	send(masterAddr, ret);
}

func (self *Daemon) start_fetch_file_get_v(m map[int]string) { 
	//Master ask for file from 5 clients with replica of the original file
	Info := m[-2]
	// file_name := parse_string(Info, 4)
	// fmt.Println(Info)
	sdfsfile_name := parse_string(Info, 5)
	version := parse_string(Info, 6)
	requestor, _ := strconv.Atoi(parse_string(Info, 0)) //vm that request for file

	VM_ID_list := self.GDHT[sdfsfile_name]
	// VM_ID_list := []int{1, 2}
	VM_ID_list_string := ""
	for i:= 0; i < len(VM_ID_list); i++{
		VM_ID_list_string += (strconv.Itoa(VM_ID_list[i]) + "###")
	}
	ID_info := VM_ID_list_string + sdfsfile_name + "###" +  parse_string(Info, 4) + "###" + version// 1###2###3###asdf###word#0
	emptyMap := make(map[int]string)
	for k,v := range self.MemberList {
	  emptyMap[k] = v
	}
	ret := change_list_to_stream(emptyMap, "require_latest_file_get_v", ID_info, change_time(time.Now()))
	requestor_vm_IP := parse_string(self.MemberList[requestor], 1)
	send(requestor_vm_IP, ret)
}

func (self *Daemon)send_latest_file_get_v(m map[int]string){
	// Each client send the required replica to the requestor
	Info := m[-2] // 1###2###3###asdf###word#0
	// fmt.Println("Info before replica num", Info)
	ID_array := strings.Split(Info, "###")
	sdfs_name := ID_array[len(ID_array) - 3]
	file_name := ID_array[len(ID_array) - 2]
	version := ID_array[len(ID_array) - 1]

	num_replica := NUM_REPLICA // 3
	// fmt.Println("replica num", num_replica)
	for i:=0; i < num_replica; i++{
		ID,_ := strconv.Atoi(ID_array[i])
		replica_IP_old := parse_string(self.MemberList[ID], 1)
		replica_IP := change_port(replica_IP_old) // change from 4040 to 8080 port
		// file_name := ID_array[len(ID_array) - 1]
		self.start_File_get_v(replica_IP, sdfs_name, file_name, version) // Ask file from servers
	}
}

func(self *Daemon)start_File_get_v(ip_address string, sdfs_name string, local_name string, version string){
    // fmt.Println("HERE2")
    // fmt.Println("ip_address ", ip_address, "self.ID ", self.ID)
    // fmt.Println("no problem, ", sdfs_name, local_name)
    conn, err := rpc.DialHTTP("tcp",ip_address)
    assert(err, "error to dial")
    defer conn.Close()
    // local_latest_name := self.LDHT[command][0]
    args := sdfs_name + "###" + version
    var reply string 
    ver,_ := strconv.Atoi(version)
    err = conn.Call("Arith.Command_grep_get", &args, &reply)
    assert(err, "Error")
    // fmt.Println("reply ", reply)
	// delete_oldest_file(sdfs_name)
    // Write_2_file(reply, sdfs_name + "_" + change_time(time.Now()) + "_" + "0_")
    file_name := strings.Split(reply, deliminator)[0]
    file_content := strings.TrimPrefix(reply, file_name + deliminator)
    if (self.fileCounter_List[ver] >= 0 && (self.fileCounter_List[ver] < int(NUM_REPLICA/2) + 1) && (Write_2_file(file_content, file_name, LOCAL) == 0)){
    	self.fileCounter_List[ver]++;
    	// self.versionCounter--;
    	// fmt.Println("versionCounter 1             ", self.versionCounter)
    }
    if (self.fileCounter_List[ver] >= 0 && self.fileCounter_List[ver] >= int(NUM_REPLICA/2) + 1){
        // fmt.Println(file_name, " adfadsafds")
    	self.filter_latest_file(file_name, local_name)
    	self.fileCounter_List[ver] = -1
    	// ver,_ := strconv.Atoi(version)
    	// fmt.Println("ver", ver)
    	self.ver_array[ver] = 1
    	// self.getVersionFileCounter ++
    }
}


// ^======================//


func (self *Daemon)put_rc(local_name string, sdfs_name string){ 
	// Add the local file to the SDFS
	//add file name to info	
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + local_name + "###" + sdfs_name
	// self.MemberList[self.ID] = Id_info
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "update_DHT_rc", Id_info, change_time(time.Now()))

	//send file update DHT info to master
	masterAddr := parse_string(self.MemberList[self.masterID], 1)
	send(masterAddr, ret);
}

func (self *Daemon)update_dht_rc(m map[int]string) {
	//Info := strconv.Itoa(self.ID)+ self.addr + (self.status)+ (time.Now()) + local_name + sdfs_name
	//update DHT in master
	Info := m[-2]
	sdfs_name := parse_string(Info, 5)
    // fmt.Println("sdfs_name ", sdfs_name)
	//get original VM_ID with file
	VM_ID, _ := strconv.Atoi(parse_string(Info, 0))
	self.GDHT[sdfs_name] = []int{}
	if (VM_ID != 0) {
		if _, ok := self.GDHT[sdfs_name]; !ok { //if not in GDHT, create

		    temp := []int{VM_ID}
		   // temp = append(temp, VM_ID)

			self.GDHT[sdfs_name] = temp
		} else { //if in GDHT, append
			self.GDHT[sdfs_name] = append(self.GDHT[sdfs_name], VM_ID);
		}
	}
	//start to send replica file
	self.send_replica_file_put_rc(Info)
}

func (self *Daemon)send_replica_file_put_rc(Info string) {
	// local_name := parse_string(Info, 4)
	VM_ID, _ := strconv.Atoi(parse_string(Info, 0))
	// self.GDHT[file_name] 
	num_rep_to_send := NUM_REPLICA - 1
	if(VM_ID == 0){
		num_rep_to_send ++ 
	}
	emptyMap := make(map[int]string)
	for k,v := range self.MemberList {
	  emptyMap[k] = v
	}
	sdfs_name := parse_string(Info, 5)
	// for vm_rep := (VM_ID + 1) % len(self.MemberList); vm_rep % len(self.MemberList) != VM_ID; vm_rep++{
	// 	member_active, _ := strconv.Atoi(parse_string(self.MemberList[vm_rep], 2))
	for temp := VM_ID % (len(self.MemberList) - 1); (temp % (len(self.MemberList) - 1)) != VM_ID - 1; temp++{
		vm_rep := (temp % (len(self.MemberList) - 1)) + 1
		// fmt.Println("vm_rep", vm_rep)
		// fmt.Println("self.MemberList[vm_rep]", self.MemberList[vm_rep])
		member_active, _ := strconv.Atoi(parse_string(self.MemberList[vm_rep], 2))
		if (member_active == 1) { 
			num_rep_to_send--;
			// from_vm_IP := parse_string(self.MemberList[VM_ID], 1) //original file
			to_vm_IP := parse_string(self.MemberList[vm_rep], 1) //replica destination 
			// fmt.Println("request file ", sdfsfile_name, " from vm IP", from_vm_IP, " to ", to_vm_IP)
			ID_info := Info
			ret := change_list_to_stream(emptyMap, "require_latest_file_put_rc", ID_info, change_time(time.Now()))
			send(to_vm_IP, ret)	
			self.GDHT[sdfs_name] = append(self.GDHT[sdfs_name], vm_rep);
		}
		if (num_rep_to_send == 0) {
		    break
		}
	}
	if (VM_ID != 0){
		ID_info := Info
		to_vm_IP := parse_string(self.MemberList[VM_ID], 1)
		ret := change_list_to_stream(emptyMap, "require_latest_file_put_rc", ID_info, change_time(time.Now()))
		send(to_vm_IP, ret)
	}
}

func (self *Daemon)send_latest_file_put_rc(m map[int]string){
	// Each client send the required replica to the requestor
	Info := m[-2] // 1###2###3###asdf###word#0
	// fmt.Println("Info before replica num rc", Info)
	ID,_ := strconv.Atoi(parse_string(Info, 0))
	// parse_array := strings.Split(Info, "###")
	sdfs_name := parse_string(Info, 5) //parse_array[len(parse_array) - 1]
	file_name := parse_string(Info, 4) //parse_array[len(pasre_array) - 2]
	replica_IP_old := parse_string(self.MemberList[ID], 1)
	replica_IP := change_port(replica_IP_old) // change from 4040 to 8080 port
	self.start_File_put_rc(replica_IP, sdfs_name, file_name) // Ask file from servers
}

func(self *Daemon)start_File_put_rc(ip_address string, sdfs_name string, local_name string){

    // flag.StringVar(&ip_address, "i", ":8080", "ip address")
    // flag.StringVar(&command, "c", "grep go server.go", "message")
    // flag.Parse()
    // fmt.Println("no problem rc")
    conn, err := rpc.DialHTTP("tcp",ip_address)
    assert(err, "error to dial")
    defer conn.Close()
    // local_latest_name := self.LDHT[command][0]
    args := local_name
    var reply string
    err = conn.Call("Arith.Command_grep_put_rc", &args, &reply)
    assert(err, "Error")
    // fmt.Println("reply ", reply)
	if (reply != "Do not"){
		delete_oldest_file(sdfs_name)
		Write_2_file(reply, sdfs_name + "_" + change_time(time.Now()) + "_" + "0_", SDFS)
	}
}



// ============================================================================================//

func (self *Daemon)put(local_name string, sdfs_name string){ 
	// Add the local file to the SDFS
	//add file name to info	
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + local_name + "###" + sdfs_name
	// self.MemberList[self.ID] = Id_info
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "update_DHT", Id_info, change_time(time.Now()))

	//send file update DHT info to master
	masterAddr := parse_string(self.MemberList[self.masterID], 1)
	send(masterAddr, ret);
}

func (self *Daemon)update_dht(m map[int]string) {
	//Info := strconv.Itoa(self.ID)+ self.addr + (self.status)+ (time.Now()) + local_name + sdfs_name
	//update DHT in master
	Info := m[-2]
	sdfs_name := parse_string(Info, 5)
    // fmt.Println("sdfs_name ", sdfs_name)
	//get original VM_ID with file
	VM_ID, _ := strconv.Atoi(parse_string(Info, 0))
	if(len(self.GDHT[sdfs_name]) == NUM_REPLICA){
		self.send_replica_file_put(Info)
	}else{
		self.GDHT[sdfs_name] = []int{}
		if (VM_ID != 0) {
			if _, ok := self.GDHT[sdfs_name]; !ok { //if not in GDHT, create

			    temp := []int{VM_ID}
			   // temp = append(temp, VM_ID)

				self.GDHT[sdfs_name] = temp
			} else { //if in GDHT, append
				self.GDHT[sdfs_name] = append(self.GDHT[sdfs_name], VM_ID);
			}
		}
		//start to send replica file
		self.send_replica_file_put(Info)
	}
}

func (self *Daemon)send_replica_file_put(Info string) {
	// local_name := parse_string(Info, 4)
	VM_ID, _ := strconv.Atoi(parse_string(Info, 0))
	// self.GDHT[file_name] 
	num_rep_to_send := NUM_REPLICA - 1
	if(VM_ID == 0){
		num_rep_to_send ++ 
	}
	emptyMap := make(map[int]string)
	for k,v := range self.MemberList {
	  emptyMap[k] = v
	}
	sdfs_name := parse_string(Info, 5)
	// for vm_rep := (VM_ID + 1) % len(self.MemberList); vm_rep % len(self.MemberList) != VM_ID; vm_rep++{
	// 	member_active, _ := strconv.Atoi(parse_string(self.MemberList[vm_rep], 2))
	for temp := VM_ID % (len(self.MemberList) - 1); (temp % (len(self.MemberList) - 1)) != VM_ID - 1; temp++{
		vm_rep := (temp % (len(self.MemberList) - 1)) + 1
		// fmt.Println("vm_rep", vm_rep)
		// fmt.Println("self.MemberList[vm_rep]", self.MemberList[vm_rep])

		if(len(self.GDHT[sdfs_name]) == NUM_REPLICA){
			list := self.GDHT[sdfs_name]
			for i := 0; i < NUM_REPLICA; i++{
				to_vm_IP := parse_string(self.MemberList[list[i]], 1) //replica destination 
				// fmt.Println("request file ", sdfsfile_name, " from vm IP", from_vm_IP, " to ", to_vm_IP)
				ID_info := Info
				ret := change_list_to_stream(emptyMap, "require_latest_file_put", ID_info, change_time(time.Now()))
				send(to_vm_IP, ret)			
			}
			return			
		}
		member_active, _ := strconv.Atoi(parse_string(self.MemberList[vm_rep], 2))
		if (member_active == 1) { 
			num_rep_to_send--;
			// from_vm_IP := parse_string(self.MemberList[VM_ID], 1) //original file
			to_vm_IP := parse_string(self.MemberList[vm_rep], 1) //replica destination 
			// fmt.Println("request file ", sdfsfile_name, " from vm IP", from_vm_IP, " to ", to_vm_IP)
			ID_info := Info
			ret := change_list_to_stream(emptyMap, "require_latest_file_put", ID_info, change_time(time.Now()))
			send(to_vm_IP, ret)	
			self.GDHT[sdfs_name] = append(self.GDHT[sdfs_name], vm_rep);
		}
		if (num_rep_to_send == 0) {
		    break
		}
	}
	if (VM_ID != 0){
		ID_info := Info
		to_vm_IP := parse_string(self.MemberList[VM_ID], 1)
		ret := change_list_to_stream(emptyMap, "require_latest_file_put", ID_info, change_time(time.Now()))
		send(to_vm_IP, ret)
	}
}

func (self *Daemon)send_latest_file_put(m map[int]string){
	// Each client send the required replica to the requestor
	Info := m[-2] // 1###2###3###asdf###word#0
	// fmt.Println("Info before replica num", Info)
	ID,_ := strconv.Atoi(parse_string(Info, 0))
	// parse_array := strings.Split(Info, "###")
	sdfs_name := parse_string(Info, 5) //parse_array[len(parse_array) - 1]
	file_name := parse_string(Info, 4) //parse_array[len(pasre_array) - 2]
	replica_IP_old := parse_string(self.MemberList[ID], 1)
	replica_IP := change_port(replica_IP_old) // change from 4040 to 8080 port
	self.start_File_put(replica_IP, sdfs_name, file_name) // Ask file from servers
}

func(self *Daemon)start_File_put(ip_address string, sdfs_name string, local_name string){

    // flag.StringVar(&ip_address, "i", ":8080", "ip address")
    // flag.StringVar(&command, "c", "grep go server.go", "message")
    // flag.Parse()
    // fmt.Println("no problem")
    conn, err := rpc.DialHTTP("tcp",ip_address)
    assert(err, "error to dial")
    defer conn.Close()
    // local_latest_name := self.LDHT[command][0]
    args := local_name
    var reply string
    err = conn.Call("Arith.Command_grep_put", &args, &reply)
    assert(err, "Error")
    // fmt.Println("reply ", reply)
	if (reply != "Do not"){
		delete_oldest_file(sdfs_name)
		Write_2_file(reply, sdfs_name + "_" + change_time(time.Now()) + "_" + "0_", SDFS)
	}
}

func delete_oldest_file(sdfs string){
    var out_file bytes.Buffer
    find_cmd := SDFS_FIND + "find . -name \"" + sdfs + "_*\"" + SORT
    file_cmd := exec.Command("/bin/bash","-c", find_cmd)
    file_cmd.Stdout = &out_file
    err := file_cmd.Run()
    ret := strings.Split(out_file.String(), "\n")
    // fmt.Println(ret)
    // Get the num of versions
    for i:=0; i < len(ret) - 1; i++{
    	partition := strings.Split(ret[i], "_")
    	file_name := partition[0] 
    	time_stamp := partition[1]
    	// fmt.Println("partition: ", partition)
    	version,err := strconv.Atoi(partition[2])
    	if err != nil{
    	    fmt.Println("File not found!")
    	}
    	if (version < 4){
    	 	cmd := SDFS_FIND + "mv " + ret[i] + " " + file_name + "_" + time_stamp + "_" + strconv.Itoa(version + 1) + "_"
    	 	run := exec.Command("/bin/bash","-c", cmd)
    	 	run.Run()
    	}else{
    		run := exec.Command("/bin/bash","-c", SDFS_FIND + "rm " + ret[i])
    		run.Run()
    	}
    }
    if err != nil {
        log.Fatalln(err)
    }
}







func (self *Daemon)delete(sdfs_name string){ 
	// Add the local file to the SDFS
	// add file name to info	
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + sdfs_name
	// self.MemberList[self.ID] = Id_info
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "delete_sdfs", Id_info, change_time(time.Now()))

	//send file update DHT info to master
	masterAddr := parse_string(self.MemberList[self.masterID], 1)
	send(masterAddr, ret);
}

func (self *Daemon)delete_SDFS(m map[int]string) {
	// master receive  "update_DHT", ask original client to send replica file

	//Info := strconv.Itoa(self.ID)+ self.addr + (self.status)+ (time.Now()) + local_name + sdfs_name
	//update DHT in master
	Info := m[-2]
	file_name := parse_string(Info, 4)
    // fmt.Println("file_name ", file_name)
    //delete file in GDHT
	delete(self.GDHT, file_name)
	
	//start to send replica file
	self.send_delete_instruction(Info)
}

func (self *Daemon)send_delete_instruction(m string) {
	sdfsfile_name := parse_string(m, 4)
	// VM_ID, _ := strconv.Atoi(parse_string(m, 0))
	num_rep_to_send := len(self.MemberList) - 1 //NUM_REPLICA
	emptyMap := make(map[int]string)
	for k,v := range self.MemberList {
	  emptyMap[k] = v
	}
	for temp := 0; temp < 10; temp++{
		vm_rep := (temp % (len(self.MemberList) - 1)) + 1
		member_active, _ := strconv.Atoi(parse_string(self.MemberList[vm_rep], 2))
		if (member_active == 1) {
			num_rep_to_send--;
			to_vm_IP := parse_string(self.MemberList[vm_rep], 1) //delete replica at destination 
			ID_info := sdfsfile_name 
			ret := change_list_to_stream(emptyMap, "delete_all_file", ID_info, change_time(time.Now()))
			send(to_vm_IP, ret)	
		}
		// if (num_rep_to_send == 0) {
		//     break
		// }
	}
}

func (self *Daemon)delete_local_file(m map[int]string) {
	file_name := m[-2]
	// fmt.Println("start delete run command", SDFS_FIND + "rm " + file_name + "_*")
    delete_cmd := exec.Command("/bin/bash","-c", SDFS_FIND + "rm " + file_name + "_*")
    err := delete_cmd.Run()
    if err != nil {
    	// fmt.Println("Fail to delete", file_name)
    }
}

func if_in_list(list []int, a int)(is_in int){
	is_in = -1
	for k:=0; k<len(list); k++ {
		if (list[k] == a){
			is_in = k
		}
	}
	return is_in
}

func if_in_strlist(list []string, a string)(is_in int){
	is_in = -1
	for k:=0; k<len(list); k++ {
		if (list[k] == a){
			is_in = k
		}
	}
	return is_in
}

func (self *Daemon)reorganize() {
	if (self.ID != self.masterID) {
		return
	}
	//1 - New member added, 2-Status change
	self.mutex.Lock()
	need_reorgranize := 0
	var inactive_list []int 
	var active_list []int 
	for k, v := range self.MemberList{
		if _, ok := self.HistoryMemberList[k]; !ok {
			need_reorgranize = 0
		} else {
			member_new_status := parse_string(v, 2)
			member_active, _ := strconv.Atoi(member_new_status)
			member_old_status := parse_string(self.HistoryMemberList[k], 2)
			if member_old_status != member_new_status {
				need_reorgranize = 1
			}
			if (member_active != 1) {
				inactive_list = append(inactive_list, k)
			} else {
				active_list = append(active_list, k)
			}
		}
	}
	for k,v := range self.MemberList {
	  self.HistoryMemberList[k] = v
	}
	self.mutex.Unlock()
	// sort.Ints(active_list)
	// fmt.Println("need_reorgranize", need_reorgranize)

	return

	if (need_reorgranize == 1) {
		// fmt.Println("need_reorgranize here", need_reorgranize)
		self.restart_parse_yaml()
		// for sdfs_name, ID_list := range self.GDHT {
		// 	need_to_send_file  := 0
		// 	var replica_list []int 
		// 	for i:=0; i<len(ID_list);i++ {
		// 		if (if_in_list(inactive_list, ID_list[i]) == 1) {
		// 			need_to_send_file = 1
		// 		} else {
		// 			replica_list = append(replica_list, ID_list[i])
		// 		}
		// 	}
		// 	// num_need_to_send := NUM_REPLICA - len(replica_list)
		// 	if (need_to_send_file == 1) {
		// 		// sort.Ints(replica_list)
		// 		Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + sdfs_name
		// 		newMap := make(map[int]string)
		// 		for k,v := range self.MemberList {
		// 		  newMap[k] = v
		// 		}
		// 		ret := change_list_to_stream(newMap, "put_file_for_recovery", Id_info, change_time(time.Now()))
		// 		replica_IP := parse_string(self.MemberList[replica_list[0]], 1)
		// 		send(replica_IP, ret)
		// 	}
		// }
	}
}

func (t *Arith) Command_grep_put_rc (command *string, reply *string) error{
	local_name := *command
    var out bytes.Buffer
    var out_file bytes.Buffer

    find_cmd := SDFS_FIND + "find . -name \"" + strings.TrimPrefix(local_name, "./") + "\"" + SORT
    file_cmd := exec.Command("/bin/bash","-c", find_cmd)
    file_cmd.Stdout = &out_file
    err := file_cmd.Run()
    ret := strings.Split(out_file.String(), "\n")
    // fmt.Println("I run this 0", find_cmd)
    if (len(ret) == 1){
    	// fmt.Println("I run this ", find_cmd)
    	// fmt.Println("sdfs file not available!")
    	*reply = "Do not"
    }else{
	    file_found := ret[len(ret) - 2]
	    cat_cmd := SDFS_FIND + "cat " + file_found
	    cmd := exec.Command("/bin/bash","-c",cat_cmd)
	    cmd.Stdout = &out
	    err = cmd.Run()
	    // *reply = out.String()
	    if err != nil {
	        *reply = "No matching result"
	    }else{
	    	*reply = out.String()
	    }
	}
    return nil
} 

func (t *Arith) Command_grep_put (command *string, reply *string) error{
	local_name := *command
    var out bytes.Buffer
    var out_file bytes.Buffer
    find_cmd := LOCAL_FIND + "find . -name \"" + local_name + "\"" + SORT
    file_cmd := exec.Command("/bin/bash","-c", find_cmd)
    file_cmd.Stdout = &out_file
    err := file_cmd.Run()
    ret := strings.Split(out_file.String(), "\n")
    if (len(ret) == 1){
    	fmt.Println("The local file is not available!")
    	*reply = "Do not"
    }else{
	    file_found := ret[len(ret) - 2]
	    cat_cmd := LOCAL_FIND + "cat " + file_found
	    cmd := exec.Command("/bin/bash","-c",cat_cmd)
	    cmd.Stdout = &out
	    err = cmd.Run()
	    // *reply = out.String()
	    if err != nil {
	        *reply = "No matching result"
	    }else{
	    	*reply = out.String()
	    }
	}
    return nil
} 

func (self *Daemon) get_version(sdfs_file_name string, num_versions string, local_file_name string){
	// fmt.Println("Start get_version!!")
	version,_ := strconv.Atoi(num_versions)
	// fmt.Println("version: ",version)
	// self.versionCounter = version
	// fmt.Println("versionCounter0", self.versionCounter)
	// self.fileCounter = -2
	// m := &sync.Mutex{}
	self.ver_array = []int{}
	self.fileCounter_List = []int{}
	for j:=0; j<version; j++{
		self.ver_array = append(self.ver_array, 0)
		self.fileCounter_List = append(self.fileCounter_List, 0)
		// version_string := strconv.Itoa(j)
		// self.get(sdfs_file_name, version_delim + version_string, version_string)
	}
		version_string := strconv.Itoa(0)
		self.get_v(sdfs_file_name, version_delim + version_string, version_string)
	// i := -1	 
		// self.fileCounter = -1
		// fmt.Println("version_string: ",version_string)
		// fmt.Println("sdfs_file_name: ",sdfs_file_name)
		// fmt.Println("version_delim + version_string: ",version_delim + version_string)
		self.getVersionFileCounter = 0
		// m.Lock()
		// go func() {
			// 
			// m.Lock()
			m := 0
			// check := 0
			for {
				time.Sleep(150 * time.Millisecond)
				// fmt.Println("self.fileCounter: ",self.fileCounter)
				// fmt.Println("check", check)
				// fmt.Println("version", version)
					// fmt.Println("m", m)
				if (self.fileCounter_List[m] == -1){
					if self.ver_array[m] == 1 {
					// check ++
						if (m < version - 1){
							version_string := strconv.Itoa(m + 1)
							self.get_v(sdfs_file_name, version_delim + version_string, version_string)	
							// time.Sleep(3000 * time.Millisecond)				
						}
						self.ver_array[m] = -1
						// self.fileCounter = 0
						m++
					}
				}
				
				
				if m == (version - 1){
					break
				}

			}
			// m.Unlock()

			self.check_version(sdfs_file_name, num_versions, local_file_name)
		// }()
		// m.Unlock()
		// self.check_version(sdfs_file_name, num_versions, local_file_name)
}

func (self *Daemon)check_version(sdfs_file_name string, num_versions string, local_file_name string){
	// for{
		// fmt.Println("versionCounter2           list store next ", self.versionCounter)
			time.Sleep(5000 * time.Millisecond)
			// self.list_store()
			// Create segamentation file
			var out_file bytes.Buffer
    	 	cat_cmd := LOCAL_FIND + "echo \"" + version_seg + "\" > " + "seg"
    	 	// fmt.Println("cat command ", cat_cmd)
    	 	run := exec.Command("/bin/bash","-c", cat_cmd)
    	 	err := run.Run()
   		    if err != nil{
			    fmt.Println("File not found!")
			}
    	 	// Cat
			find_cmd := LOCAL_FIND + "find . -name \"" + version_delim + "*\"" + SORT
			// fmt.Println("find_cmd", find_cmd)
		    local_name := exec.Command("/bin/bash","-c", find_cmd)
		    local_name.Stdout = &out_file
		    err = local_name.Run()
		    if err != nil{
			    fmt.Println("File not found!!")
			}
			diff_version_file := strings.Split(out_file.String(), "\n")
			// fmt.Println("num_diff", len(diff_version_file))
			sort.Strings(diff_version_file)
			// fmt.Println("diff_version_file", diff_version_file)
			cat_cmd = ""
			// for i := 0; i < len(diff_version_file) - 1; i++{
			for i := 1; i < len(diff_version_file); i++{
				cat_cmd += diff_version_file[i] + " seg "
			}
			cat := LOCAL_FIND + "cat " + cat_cmd + "> " + local_file_name
			run = exec.Command("/bin/bash","-c", cat)
			// fmt.Println("another cat", cat)
		    err = run.Run()
		    if err != nil{
			    fmt.Println("File not found!!!")
			}

			// Delete temp files
			delete_cmd := LOCAL_FIND + "rm " + version_delim + "*" + " seg"
			// fmt.Println("try to delete ", delete_cmd)
			run = exec.Command("/bin/bash","-c", delete_cmd)
		    err = run.Run()
		    if err != nil{
			    fmt.Println("Can not delete!!!!")
			}
}


func (self *Daemon)list_file(sdfs_name string){
	//add file name to info	
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + sdfs_name
	// self.MemberList[self.ID] = Id_info
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "request_ls_list", Id_info, change_time(time.Now()))

	//send file update DHT info to master
	masterAddr := parse_string(self.MemberList[self.masterID], 1)
	send(masterAddr, ret)
}	

func (self *Daemon)send_ls_list(m map[int]string){
	Info := m[-2]
	sdfsname := parse_string(Info, 4)
	Id_info := ""
	for i := 0; i < len(self.GDHT[sdfsname]); i++{
		Id_info += strconv.Itoa(self.GDHT[sdfsname][i]) + " "
	}
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "display_ls_list", Id_info, change_time(time.Now()))
	addr := parse_string(self.MemberList[self.masterID], 1)
	send(addr, ret)
}

func (self *Daemon)display_ls_list(m map[int]string){
	fmt.Println("VMs that store: ")
	fmt.Println(m[-2])
}

func (self *Daemon)list_store(){
	cmd := SDFS_FIND + "ls"
	var out_file bytes.Buffer
	run := exec.Command("/bin/bash","-c", cmd)
	run.Stdout = &out_file
    err := run.Run()
    if err != nil{
	    fmt.Println("Can not ls!!")
	}
	fmt.Println(out_file.String())
}

func (self *Daemon) send_time_to_compare(m map[int]string) {

}

func (self *Daemon)print_sdfs_not_available(){
	fmt.Println("Sorry, the file is not available.")
}

func updatetime(m map[int]string, info string, time string)(m_new map[int]string){
    // fmt.Println("info  ", info)
	id,_ := strconv.Atoi(parse_string(info, 0))
// 	fmt.Println("idid ", id)
	info_temp := m[id]
// 	fmt.Println("info_tempinfo_temp ", info_temp)
	info_new := parse_string(info_temp, 0)+"###"+parse_string(info_temp, 1)+"###"+parse_string(info_temp, 2)+"###"+time
	m_new = m
	m_new[id] = info_new
	return
}

func GetLocalAddr(port int) (addr net.UDPAddr) {
 
	name, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	ip, err := net.LookupHost(name)
	if err != nil {
		log.Fatal(err)
	}

	addr = net.UDPAddr{
        Port: port,
        IP: net.ParseIP(ip[0]),
    } 
	return
}

func CreateUDPListener(addr net.UDPAddr)(ser *net.UDPConn){
	// listen to incoming udp packets
    ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        log.Fatal(err)
    }
	return
}


func NewDaemon(Port int, ID int)(daemon *Daemon){
	daemon = new(Daemon)
	temp := GetLocalAddr(Port)
	daemon.Listener = CreateUDPListener(temp)
	// temp1 := GetLocalAddr(6060)
	daemon.Storm_listener, _ = net.Listen("tcp", ":6060")
// 	daemon.start_File_server()
	sport := strconv.Itoa(temp.Port)
	daemon.addr = (temp.IP).String() +":"+sport
	var s []int
	daemon.NeighborList = s
	daemon.MemberList = make(map[int]string)
	daemon.MemberList[ID] = strconv.Itoa(ID)+"###"+daemon.addr+"###"+"1"+"###"+change_time(time.Now())
	daemon.port = Port
	daemon.ID = ID
	daemon.random_count = 0
	daemon.status = 1
	daemon.masterID = 0
	daemon.masterIP = []string{"172.22.154.82:4040", "172.22.156.82:4040"}
	daemon.GDHT = make(map[string][]int)
	daemon.fail_counter = make(map[int]int)
	daemon.timestamp = time.Now()
	daemon.HistoryMemberList = make(map[int]string)
	daemon.HistoryMemberList[ID] = strconv.Itoa(ID)+"###"+daemon.addr+"###"+"1"+"###"+change_time(time.Now())
	// daemon.scanning()
	daemon.fileCounter = 0
	daemon.funk = -1
	daemon.op = -1
	daemon.tar = ""
	daemon.num_child = 0
	daemon.child = []int{}
	daemon.robin = 0
	daemon.task_queue = []string{}
	daemon.outputMap = make(map[string]int)
	daemon.outputString = ""
	// daemon.versionCounter = 0
	return
}


// helper function
func send(address string, m map[int]string){
	// Send message
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
    err := encoder.Encode(m)
    // fmt.Println("Expected bytes", buf)
    if err != nil {
        log.Fatal(err)
    }

    conn, err := net.Dial("udp", address)
    defer conn.Close()
    if err != nil{
    	log.Fatal(err)
    }
    conn.Write(buf.Bytes())
    
    return	
}


// func (self *Daemon)quanbuchonglai(){
// 	self.send_tcp
// }

// func (self *Daemon)send_tcp(address string, m map[int]string, id_to_send int){
// 	// fmt.Println("send_tcp_map!!!!!!!!!!!!!!!!!", m)
// 	// Send message
// 	if (parse_string(self.MemberList[id_to_send], 2) != "1"){
// 		if(m[-1] == "process"){
// 			self.mutex_task.Lock()
// 			self.task_queue = append(self.task_queue, m[-2])
// 			self.mutex_task.Unlock()
// 		}
// 		return
// 	}
// 	var buf bytes.Buffer
// 	encoder := gob.NewEncoder(&buf)
//     err := encoder.Encode(m)
//     // fmt.Println("Expected bytes", buf)
//     if err != nil {
//         log.Fatal(err)
//     }
//     // fmt.Println("dial tcp address ", address)
//     conn, err := net.Dial("tcp", address)
//     // 
//     if err != nil{
//     	fmt.Println("Fail to dial address: ", address)
//     	// log.Fatal(err)
//     }else{
//     	defer conn.Close()
//     	conn.Write(buf.Bytes())
//     }
//     return	
// }

func (self *Daemon)send_tcp(address string, m map[int]string, id_to_send int)(success int){
	// fmt.Println("send_tcp_map!!!!!!!!!!!!!!!!!", m)
	// time.Sleep(500)
	// Send message
	success = 1
	if (parse_string(self.MemberList[id_to_send], 2) != "1"){
		return
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
    err := encoder.Encode(m)
    // fmt.Println("Expected bytes", buf)
    if err != nil {
        log.Fatal(err)
    }
    // fmt.Println("dial tcp address ", address)
    conn, err := net.Dial("tcp", address)
    // 
    if err != nil{
    	// fmt.Println("Fail to dial address: ", address)
    	// log.Fatal(err)
    	return
    }else{
    	defer conn.Close()
    	/*_, err*/conn.Write(buf.Bytes())
    	// if err != nil{
    	// 	return
    	// }
    }


    success = 0
    return
}

func (self *Daemon) send_ack(m map[int]string){
	info := m[-2]
	self.status = 1
	addr := parse_string(info, 1)
	time_now := change_time(time.Now())
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + time_now
	self.mutex.Lock()
	self.MemberList[self.ID] = Id_info
	self.mutex.Unlock()
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "update", Id_info,	time_now)
	send(addr, ret)
}

func (self *Daemon) send_ping(){
    self.status = 1
    if len(self.NeighborList) == 0  {
        return
    }
    
    if self.random_count < len(self.NeighborList) - 1 {
        self.random_count ++
    } else {
        self.random_count = 0
    }
	neighbor_idx := self.random_count
	neighbor := self.NeighborList[neighbor_idx]
	selected_member := self.MemberList[neighbor]
	
	addr := parse_string(selected_member, 1)
	id,_ := strconv.Atoi(parse_string(selected_member, 0))
	
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now())
	self.mutex.Lock()
	self.MemberList[self.ID] = Id_info
	self.mutex.Unlock()
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "send_ack", Id_info, change_time(time.Now()))
	self.mutex.Lock()
	if (id != self.ID){
		
		self.fail_counter[id] ++ // Sent
		
	}
	// self.fail_counter[id] ++ // Sent
	self.mutex.Unlock()
	send(addr, ret)	
	return
}


func change_time(t time.Time) (timestring string){
    timeInt := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
    timestring = timeInt
    return
}

func gettime(info string)(timeInt int){
	timeInt, err := strconv.Atoi(info)
	if err != nil {
	    fmt.Println("P error: ",err)
	}
	return
}


func Write_2_File(s string, path string, file_name string){
    if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}
    f, err := os.OpenFile(path + file_name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
	    panic(err)
	}
	defer f.Close()
	if _, err = f.WriteString(s + "\n"); err != nil {
	    panic(err)
	}
}


func (self *Daemon) updateLogFile(){
	for k, v := range self.MemberList{
		if _, ok := self.HistoryMemberList[k]; !ok {
		    t := parse_string(v, 3)
			//New member added
			record :="At time:" + time.Unix(0, int64(gettime(t))).String() + " $$ VM "+ strconv.Itoa(k) + " join the group"
			Write_2_File(record, "./result", "/result_vm" + strconv.Itoa(getMyID() + 1) + ".txt")
		} else {
			member_new_status := parse_string(v, 2)
			member_old_status := parse_string(self.HistoryMemberList[k], 2)
			if member_old_status != member_new_status {
			    t := parse_string(v, 3)
				record :="At time:" + time.Unix(0, int64(gettime(t))).String() + "$$ status of VM "+ parse_string(v, 0) + " changes from " + member_old_status + " to " + member_new_status
				Write_2_File(record, "./result", "/result_vm" + strconv.Itoa(getMyID() + 1) + ".txt")
			}
		}
	}
	for k,v := range self.MemberList {
	  self.HistoryMemberList[k] = v
	}
}

func (self *Daemon) update(m map[int]string){
	self.mutex.Lock()
	info := m[-2]
	id,_ := strconv.Atoi(parse_string(info, 0))
	if (id != self.ID){
		
		self.fail_counter[id] = 0 // Received
		
	}
	for k, _ := range m {
		if k >= 0 {
			if _, ok := self.MemberList[k]; !ok {
				self.MemberList[k] = m[k]
			}
		}
	}
	for k, v := range self.MemberList{
		if k >= 0 {
			if _, ok := m[k]; !ok {
				continue
			}            

			self_time := gettime(parse_string(v, 3))
			m_time := gettime(parse_string(m[k], 3))
			if self_time - m_time <= 0{
				// self.mutex.Lock()
				self.MemberList[k] = m[k]
				// self.mutex.Unlock()
			}
		}
	}
	new_my := self.MemberList[self.ID]
	// fmt.Println("nwenn ", new_my, self.ID)
	self.MemberList[self.ID] = parse_string(new_my, 0)+"###"+parse_string(new_my, 1)+"###"+parse_string(new_my, 2)+"###"+change_time(time.Now())
	self.update_neighbor()
	self.mutex.Unlock()
}

func contains(s []int, e int) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}

func(self *Daemon) update_neighbor() {
    // Update Neighbor List
	var s []int
	self.NeighborList = s
	for i,_:= range(self.MemberList){
		self.NeighborList = append(self.NeighborList, i)
	}
	// for i := (self.ID + 1) % 10; i % 10 != self.ID; i++{
	// 	if _, ok := self.MemberList[i % 10]; !ok {
	// 		continue
	// 	}
	// 	temp2,_ := strconv.Atoi(parse_string(self.MemberList[i % 10], 2))
 // 		member_active := (temp2 == 1) 
	// 	is_introducer := (i % 10 == 0)
	// 	if member_active && !is_introducer {
	// 		temp1,_ := strconv.Atoi(parse_string(self.MemberList[i % 10], 0))
	// 		self.NeighborList = append(self.NeighborList, temp1)
	// 		count ++;
	// 	}
	// 	if count >= limit - 1{
	// 		break;
	// 	}
	// }
	// if self.ID != 0 {
	// 	self.NeighborList = append(self.NeighborList, 0)
	// } else {
	//     self.NeighborList = []int{}
	//     for k, v := range self.MemberList {
	//         temp2,_ := strconv.Atoi(parse_string(v, 2))
 //     		member_active := (temp2 == 1) 
	// 	    if (member_active) {
 //    			self.NeighborList = append(self.NeighborList, k)
 //    		}
	// 	}
	// }
}

func (self *Daemon) send_join(addr string){
    self.status = 1
	m := make(map[int]string)
	sid := strconv.Itoa(self.ID)
	m[0] = sid+"###"+self.addr+"###"+"1"+"###"+change_time(time.Now())
	self.mutex.Lock()
	self.MemberList[self.ID] = sid+"###"+self.addr+"###"+"1"+"###"+change_time(time.Now())
	// send(self.addr, m)
	self.mutex.Unlock()
	send(addr, m)
}

func (self *Daemon) send_leave(){
	neighbor_idx := rand.Int() % len(self.NeighborList)

	
	neighbor := self.NeighborList[neighbor_idx]
	selected_member := self.MemberList[neighbor]
	
	addr := parse_string(selected_member, 1)
	time_now := change_time(time.Now())

	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(2)  + "###" + time_now
	self.MemberList[self.ID] = Id_info
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}

	ret := change_list_to_stream(newMap, "update", Id_info, time_now)
	send(addr, ret)
	send(self.masterIP[self.masterID], ret)
	send(self.masterIP[1 - self.masterID], ret)
	self.status = 2	
}

func (self *Daemon) remove_sdfs() {
	// fmt.Println("start remove sdfs directory", "rm sdfs_dir/*")
    delete_cmd := exec.Command("/bin/bash","-c", SDFS_FIND + "rm *")
    err := delete_cmd.Run()
    if err != nil {
    	// fmt.Println("Fail to remove")
    }
}

func (self *Daemon) checkstdin(done1 chan bool){
	var inputReader *bufio.Reader
	// var input string
	go self.READ()
	go self.read_storm()
	for{
		inputReader = bufio.NewReader(os.Stdin)
		input,_ := inputReader.ReadString('\n')
		// input := input0.String()
		// fmt.Println(input)
		if(input == "leave\n"){
			fmt.Println("You voluntarily leave the group!")
			self.status = 2
			time_now := change_time(time.Now())
			Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(2)  + "###" + time_now
	        self.MemberList[self.ID] = Id_info
			self.send_leave()
			done1 <- true
		}
		if(input == "join\n"){
			fmt.Println("Congrats! You've been in the group!")
			self.status = 1
			self.send_join(self.masterIP[self.masterID])
			self.send_join(self.masterIP[1 - self.masterID])
			// self.remove_sdfs()
		}

		if(input == "mlist\n"){
			self.print_mlist()
		}
		if(input == "nlist\n"){
		    if self.random_count == len(self.NeighborList) - 1 {
        	    r := rand.New(rand.NewSource(time.Now().Unix()))
                var s []int
                for _, i := range r.Perm(len(self.NeighborList)) {
                  s = append(s, self.NeighborList[i])
                }
                self.NeighborList = s
            }
			self.print_nlist()
		}
		if (input == "exit\n") {
			fmt.Println("You have been failed!")
			self.status = 0
			done1 <- true
		}
        if (input == "id\n") {
        	// fmt.Println(input)
            fmt.Println("VM ID is ", self.ID)
        }
        if (input == "GDHT\n"){
        	fmt.Println("GDHT is ", self.GDHT)
        }
        // if (input == "store\n"){
        if (strings.Contains(input, "store")){
        	self.list_store()
        }
        if (strings.Contains(input, "put")) {
        	InputResult := strings.Split(input, " ") 
        	local_file_name := InputResult[1] 
        	sdfs_file_name := strings.TrimSuffix(InputResult[2], "\n") 
        	self.put(local_file_name, sdfs_file_name)
        }
        if (strings.Contains(input, "get") && !strings.Contains(input, "get-versions")) {
        	// fmt.Println(input)
        	InputResult := strings.Split(input, " ") 
        	// fmt.Println(InputResult)
        	sdfs_file_name := InputResult[1] 
        	local_file_name := InputResult[2] 
        	// version := strings.TrimSuffix(InputResult[3], "\n") 
        	self.get(sdfs_file_name, local_file_name, "0")
        }
        if(strings.Contains(input, "get-versions")){
        	// fmt.Println(input)
        	InputResult := strings.Split(input, " ") 
        	// fmt.Println(InputResult)
        	fmt.Println("Start get version!")
        	sdfs_file_name := InputResult[1] 
        	num_versions := InputResult[2] 
        	local_file_name := strings.TrimSuffix(InputResult[3], "\n") 
        	self.get_version(sdfs_file_name, num_versions, local_file_name)
        }
        if (strings.Contains(input, "delete")) {
        	// fmt.Println(input)
        	InputResult := strings.Split(input, " ") 
        	// fmt.Println(InputResult)
        	sdfs_file_name := strings.TrimSuffix(InputResult[1], "\n") 
        	self.delete(sdfs_file_name)
        }
        if (strings.Contains(input, "ls")){
        	InputResult := strings.Split(input, " ")
        	sdfs_file_name := strings.TrimSuffix(InputResult[1], "\n")
        	self.list_file(sdfs_file_name) 
        }
        if (strings.Contains(input, "msID")){
        	fmt.Println(self.masterID)
        }
        if (strings.Contains(input, "return")){
        	InputResult := strings.Split(input, " ")
        	option := strings.TrimSuffix(InputResult[1], "\n")
        	if (option == "1"){
        		fmt.Println("length of outputstring", len(self.outputString))
        		outName := strings.TrimSuffix(InputResult[2], "\n")
        		Write_2_file(self.outputString, outName, "output")
        	}else if(option == "2"){
        		key := strings.TrimSuffix(InputResult[2], "\n")
        		if (key == "ALL") {
        			fmt.Println(self.outputMap)
        		}else{
        			fmt.Println(self.outputMap[key])
        		}
        		outName := strings.TrimSuffix(InputResult[3], "\n")
        		f, _ := os.OpenFile("output/"+outName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
				defer f.Close()
				w := bufio.NewWriter(f)
				if (key == "ALL") {
        			fmt.Println(self.outputMap)
        		}else{
        			fmt.Println(self.outputMap[key])
        		}
				w.Flush()
        	}
        }
        if (strings.Contains(input, "crane")){
        	InputResult := strings.Split(input, " ") 
        	yaml_path := strings.TrimSuffix(InputResult[1], "\n")
        	// testyml := InputResult[1]
        	// fmt.Println(testyml)
        	// self.LOCAL_FILE = strings.TrimSuffix(InputResult[2], "\n")
        	// fmt.Println(self.LOCAL_FILE)
        	if (self.ID == self.masterID) {
        		self.clientID = self.ID
        		self.task_file = yaml_path
        		self.parse_yaml(yaml_path)
    		}else{
    			self.clientID = self.ID
    			self.task_file = yaml_path
			    newMap := make(map[int]string)
				newMap[-1] = "master_parse_yaml:" + strconv.Itoa(self.ID)

				fmt.Println("Parsing yaml!")
				source, err := ioutil.ReadFile(yaml_path)
			    if err != nil {
			        fmt.Println("Read yml file failed1!")
			    }
			    fmt.Println("Doing task:", yaml_path)
				task := string(source[:])	
				newMap[-2] = task
				// fmt.Println("self Memberlist: ",self.MemberList)
				ip_addr := parse_string(self.MemberList[self.masterID], 1)
				ip_to_send := change_port_to_6060(ip_addr)

			    self.send_tcp(ip_to_send, newMap, self.masterID)
    		}
        }
        if (strings.Contains(input, "jobs")){
        	fmt.Println(self.count_jobs)
        }
	}
}

func (self *Daemon) checkfail(){
	self.mutex.Lock()
    for k, v := range self.fail_counter{
		if v >= 2{
			new_my := self.MemberList[k]
			if parse_string(new_my, 2) == "1"{
				// fmt.Println("check fail update member new_my", new_my)
				// fmt.Println("self.fail_counter", self.fail_counter)
				self.MemberList[k] = parse_string(new_my, 0)+"###"+parse_string(new_my, 1)+"###"+strconv.Itoa(0)+"###"+change_time(time.Now())								
				// fmt.Println("check fail update member", self.MemberList[k])			
			}
		}
	}
	for k, v := range self.MemberList{
	        t_in_ml := gettime(parse_string(v, 3))
    		m_time := gettime(change_time(time.Now()))
    		new_my := self.MemberList[k]
    		if m_time - t_in_ml > 2*T_fail && parse_string(v, 2) == "1"{
    			self.MemberList[k] = parse_string(new_my, 0)+"###"+parse_string(new_my, 1)+"###"+strconv.Itoa(0)+"###"+strconv.Itoa(t_in_ml)
    		}
	}
	if (len(self.MemberList[self.masterID]) !=0 && parse_string(self.MemberList[self.masterID], 2) == "0") {
		self.masterID = 1 - self.masterID
	}
	self.mutex.Unlock()
}

func change_list_to_stream(m map[int]string, action string, info string, time string)(res map[int]string){
	res = updatetime(m, info, time)
	m[-1] = action // "send_ack" "send_leave" "update" "send_join"
	m[-2] = info // id + addr + status + time
	m[-3] = time
	return
}

func (self *Daemon)AddDaemon(message string){
	id := strings.Split(message, "###")[0]
	ID, err := strconv.Atoi(id)
	if err != nil{
		log.Fatal(err)
	}
	self.mutex.Lock()
	self.MemberList[ID] = message // ID + address + status
	self.mutex.Unlock()
} 

func (self *Daemon)Start(){ //only for introducer
	// if self.ID == 
	for _, v := range self.MemberList{
		addr := parse_string(v, 1)
		time_now := change_time(time.Now())
		sid := strconv.Itoa(self.ID)
		s := sid+"###"+self.addr+"###"+"1"+"###"+time_now
		newMap := make(map[int]string)
		for k,v := range self.MemberList {
		  newMap[k] = v
		}

		m := change_list_to_stream(newMap, "update", s, time_now)
		send(addr, m)
	}
}

func (self *Daemon) start_recover_put(m map[int]string) {
	// Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now()) + "###" + sdfs_name
	sdfs_name := parse_string(m[-2], 4)
	local_name_without_suffix := sdfs_name
	var out_file bytes.Buffer
	find_cmd := "find . -name \"" + local_name_without_suffix + "_*_0_" + "\"" + SORT
    file_cmd := exec.Command("/bin/bash","-c", SDFS_FIND + find_cmd)
    file_cmd.Stdout = &out_file
    file_cmd.Run()
    local_name := strings.Split(out_file.String(), "\n")[0]
	self.put_rc(local_name, sdfs_name)
}

func (self *Daemon) READ()(err error){
	for {
		m := make(map[int]string)
		Listener := self.Listener
		inputBytes := make([]byte, 4096)
	  	length, _, err := Listener.ReadFromUDP(inputBytes)
	  	if err != nil{
	  		return err
	  	}
	  	buffer := bytes.NewBuffer(inputBytes[:length])
	  	decoder := gob.NewDecoder(buffer)
	  	decoder.Decode(&m)
	  	self.parse_read(m)
  	}
  	return nil
}

func (self *Daemon) parse_read(m map[int]string){
    // if rand.Float64() < 0.003 {
    //     return 
    // }
	if len(m) == 1{ //introducer receive send join 
		for _, v := range m{
			self.AddDaemon(v) //add other member into introducer mlist
			self.Start() //broadcast introducer mlist to all members in mlist
		}
	}else if m[-1] == "send_ack"{
		self.send_ack(m)
	}else if m[-1] == "send_leave"{
		self.update(m)
	}else if m[-1] == "update"{
		self.update(m)
	} else if m[-1] == "update_DHT" {
		// self.update_dht(m)
	} else if m[-1] == "update_DHT_rc" {
		// self.update_dht_rc(m)
	} else if m[-1] == "ask_for_file_get" {
		// self.start_fetch_file_get(m)
	} else if m[-1] == "require_latest_file_get"{
		// self.send_latest_file_get(m)
	} else if m[-1] == "ask_for_file_get_v" {
		// self.start_fetch_file_get_v(m)
	} else if m[-1] == "require_latest_file_get_v"{
		// self.send_latest_file_get_v(m)
	} else if m[-1] == "require_latest_file_put"{
		// self.send_latest_file_put(m)
	} else if m[-1] == "require_latest_file_put_rc"{
		// self.send_latest_file_put_rc(m)
	} else if m[-1] == "delete_sdfs" {
		// self.delete_SDFS(m)
	} else if m[-1] == "delete_all_file" {
		// self.delete_local_file(m)
	} else if m[-1] == "put_file_for_recovery"{
		// self.start_recover_put(m)
		// time.Sleep(T_read * time.Millisecond)
		// self.start_recover_put(m)
	} else if m[-1] == "request_ls_list"{
		// self.send_ls_list(m)
	} else if m[-1] == "display_ls_list"{
		// self.display_ls_list(m)
	} else if m[-1] == "print_sdfs_not_available" {
		// self.print_sdfs_not_available()
	} else{
		// fmt.Println(m[-1])
		fmt.Println("Invalid Input!")
		os.Exit(3)
	}
}

func(self *Daemon) handle(done2 chan bool){
	T := time.Now()
	for {
		curr_Time := time.Now().Sub(T)
		if curr_Time >= T_read * time.Millisecond{
            self.checkfail()
            self.reorganize()
            self.send_ping()
            T = time.Now()
            // self.updateLogFile()
            if (self.status == 2){
                m := make(map[int]string)
                m[self.ID] = strconv.Itoa(self.ID)+"###"+self.addr+"###"+"2"+change_time(time.Now())
                ret := change_list_to_stream(m, "update",strconv.Itoa(self.ID)+"###"+self.addr+"###"+"2"+"###" + change_time(time.Now()) , change_time(time.Now()))
                send("172.22.154.82:4040", ret)
            }
		}
		if self.status == 0 {
			done2 <- true	
		}
	}
}

func (self *Daemon) print_mlist(){
	fmt.Println("                       MemberList                   ")
	for i, v := range(self.MemberList) {
		fmt.Println("#====================== Element",i,"=======================#")
		id := parse_string(v, 0)
		addr := parse_string(v, 1)
		status := parse_string(v, 2)
		fmt.Println("Id:",id, "; Address:",addr,"; Status:",status)
		t := parse_string(v, 3)
		fmt.Println("Time:", time.Unix(0, int64(gettime(t))))
// 		fmt.Println("Time:",t)
		
	}
	fmt.Println("#=========================================================#")
}

func (self *Daemon) print_nlist(){
	fmt.Println("                   NeighborList                  ")
	fmt.Println("#==================================================#")
	fmt.Println("  ",self.NeighborList)
	fmt.Println("#==================================================#")
	fmt.Println(self.fail_counter)
}
func main(){
	MyID := getMyID()
	fmt.Println("VM ID is ", MyID)
	t0 := NewDaemon(4040, MyID)
	done1 := make(chan bool, 1)
   	done2 := make(chan bool, 1)	
    // done3 := make(chan bool, 1)
    
	go t0.checkstdin(done1)
	go t0.handle(done2)
	// go t0.start_File_server(done3)
	<-done1
	<-done2
	// <-done3

}



