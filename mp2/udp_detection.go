package main
import(
	"time"
	"fmt"
	"os"
	"net"
	"log"
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"math/rand"
// 	"io/ioutil"
)
func parse_string(s string, idx int)(ret string){
	ret = strings.Split(s, "###")[idx]
	return ret
}

func getMyID()(perline int){
        file, _ := os.Open("/home/jiayil5/local_info/info.txt")
        fmt.Fscanf(file, "%d\n", &perline)
	return
}



const (
	T_read = 500
	limit = 4
	T_fail = 6000000000
	layout = "2018-10-07 18:18:11.99876407 -0500 CDT m=+0.002631354"
)

type Daemon struct {
	// is_master int
	// DHT map[int]int
	Listener *net.UDPConn
	addr string
	NeighborList []int
	MemberList map[int]string // id + address + status + time
	port int
	ID int
	status int // 0 failed; 1 alive; 2 leave; 3 rejoin
	fail_counter map[int]int
	random_count int
	HistoryMemberList map[int]string 
	timestamp time.Time
}

func updatetime(m map[int]string, info string, time string)(m_new map[int]string){
	id,_ := strconv.Atoi(parse_string(info, 0))
	info_temp := m[id]
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
	daemon.fail_counter = make(map[int]int)
	daemon.timestamp = time.Now()
	daemon.HistoryMemberList = make(map[int]string)
	daemon.HistoryMemberList[ID] = strconv.Itoa(ID)+"###"+daemon.addr+"###"+"1"+"###"+change_time(time.Now())
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
    if err != nil{
    	log.Fatal(err)
    }
    conn.Write(buf.Bytes())
    return	
}

func (self *Daemon) send_ack(m map[int]string){
	info := m[-2]
	self.status = 1
	addr := parse_string(info, 1)
	time_now := change_time(time.Now())
	Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + time_now
	self.MemberList[self.ID] = Id_info
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
	self.MemberList[self.ID] = Id_info
	newMap := make(map[int]string)
	for k,v := range self.MemberList {
	  newMap[k] = v
	}
	ret := change_list_to_stream(newMap, "send_ack", Id_info, change_time(time.Now()))
	self.fail_counter[id] ++ // Sent
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

// func Write_2_File(s string, path string, filename string){
//     if _, err := os.Stat(path); os.IsNotExist(err) {
// 		os.Mkdir(path, 0700)
// 	}
// 	var b []byte
//         b = []byte(s)
//         err := ioutil.WriteFile(path + filename, b, 0600) //0644
//         if err != nil {
//                 log.Fatalln(err)
//         }
// }
func Write_2_File(s string, path string, filename string){
    if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}
    f, err := os.OpenFile(path + filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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
				self.MemberList[k] = m[k]
			}
		}
	}
	new_my := self.MemberList[self.ID]
	// fmt.Println("nwenn ", new_my, self.ID)
	self.MemberList[self.ID] = parse_string(new_my, 0)+"###"+parse_string(new_my, 1)+"###"+parse_string(new_my, 2)+"###"+change_time(time.Now())
	self.update_neighbor()
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
	count := 0
	for i := (self.ID + 1) % 10; i % 10 != self.ID; i++{
		if _, ok := self.MemberList[i % 10]; !ok {
			continue
		}
		temp2,_ := strconv.Atoi(parse_string(self.MemberList[i % 10], 2))
 		member_active := (temp2 == 1) 
		is_introducer := (i % 10 == 0)
		if member_active && !is_introducer {
			temp1,_ := strconv.Atoi(parse_string(self.MemberList[i % 10], 0))
			self.NeighborList = append(self.NeighborList, temp1)
			count ++;
		}
		if count >= limit - 1{
			break;
		}
	}
	if self.ID != 0 {
		self.NeighborList = append(self.NeighborList, 0)
	} else {
	    self.NeighborList = []int{}
	    for k, v := range self.MemberList {
	        temp2,_ := strconv.Atoi(parse_string(v, 2))
     		member_active := (temp2 == 1) 
		    if (member_active) {
    			self.NeighborList = append(self.NeighborList, k)
    		}
		}
	}
}

func (self *Daemon) send_rejoin(m map[int]string){
	for k, v := range self.MemberList{
		self_time := gettime(parse_string(v, 3))
		m_time := gettime(parse_string(m[k], 3))
		if self_time - m_time < 0{
			self.MemberList[k] = m[k]
		}
	}
	info := m[-2]
	id,_ := strconv.Atoi(parse_string(info, 0))
	new_my := self.MemberList[id]
	self.MemberList[id] = parse_string(new_my, 0)+"###"+parse_string(new_my, 1)+"###"+strconv.Itoa(1)+"###"+parse_string(new_my, 3)
	// Random select one neighbor
	for neighbor_idx := 1; neighbor_idx <= len(self.NeighborList); neighbor_idx++{
		neighbor := self.NeighborList[neighbor_idx]
		selected_member := self.MemberList[neighbor]
		addr := parse_string(selected_member, 1)
		Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(self.status)  + "###" + change_time(time.Now())
		newMap := make(map[int]string)
		for k,v := range self.MemberList {
		  newMap[k] = v
		}
		ret := change_list_to_stream(newMap, "update", Id_info, change_time(time.Now()))
		send(addr, ret)	
	}
}

func (self *Daemon) send_join(addr string){
    self.status = 1
	m := make(map[int]string)
	sid := strconv.Itoa(self.ID)
	m[0] = sid+"###"+self.addr+"###"+"1"+"###"+change_time(time.Now())
	self.MemberList[self.ID] = sid+"###"+self.addr+"###"+"1"+"###"+change_time(time.Now())
	// send(self.addr, m)
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
	for i:=1; i<=10; i++ {
		send("172.22.154.82:4040", ret)
	}
	self.status = 2	
}


func (self *Daemon) checkstdin(done1 chan bool){
	var input string
	go self.READ()
	for{
		fmt.Scanln(&input)
		if(input == "leave"){
			fmt.Println("You voluntarily leave the group!")
			
			self.status = 2
			time_now := change_time(time.Now())
			Id_info := strconv.Itoa(self.ID) + "###" + self.addr + "###" + strconv.Itoa(2)  + "###" + time_now
	        self.MemberList[self.ID] = Id_info
	
			for i:=1; i <=4; i ++ {
			    self.send_leave()
			}
 			time.Sleep(T_read * time.Millisecond)
			self.send_leave()
			done1 <- true
		}
		if(input == "join"){
			fmt.Println("Congrats! You've been in the group!")
			self.status = 1
			for i := 1; i <=9; i++ {
			    self.send_join("172.22.154.82:4040")
			}
// 			self.send_join("172.22.154.82:4040")
			time.Sleep(T_read * time.Millisecond)
			self.send_join("172.22.154.82:4040")
			time.Sleep(3 * T_read * time.Millisecond)
			self.send_join("172.22.154.82:4040")
		}
		if(input == "mlist"){
			self.print_mlist()
		}
		if(input == "nlist"){
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
		if (input == "exit") {
			fmt.Println("You have been failed!")
			self.status = 0
			done1 <- true
		}
                if (input == "id") {
                        fmt.Println("VM ID is ", self.ID)
                }
	}
}

func (self *Daemon) checkfail(){
    for k, v := range self.fail_counter{
		if v == 2{
			new_my := self.MemberList[k]
			if parse_string(new_my, 2) == "1"{
				self.MemberList[k] = parse_string(new_my, 0)+"###"+parse_string(new_my, 1)+"###"+strconv.Itoa(0)+"###"+change_time(time.Now())								
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
	self.MemberList[ID] = message // ID + address + status
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
	}else if m[-1] == "send_rejoin"{
		self.send_rejoin(m)
	}else{
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
            self.send_ping()
            
            T = time.Now()
            self.updateLogFile()
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

	go t0.checkstdin(done1)
	go t0.handle(done2)
	<-done1
	<-done2
}



