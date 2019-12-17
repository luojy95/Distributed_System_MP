
import (
    "net/rpc"
    "fmt"
    "log"
    "net"
    "strings"
    "flag"
    "net/http"
    "bytes"
    "os/exec"
    "net/rpc"
    "io/ioutil"
)

type Arith int

func start_File_server(){
    arith := new(Arith)
    rpc.Register(arith)
    rpc.HandleHTTP()
    my_ip:= get_ip()
    my_ip = strings.TrimSuffix(my_ip, "\n")

    // Create listener
    listener, err := net.Listen("tcp", my_ip + ":8080")

    // Assert error
    // assert(err, "Can not create listener")
    fmt.Println("Can not create listener")

    // Clean up listener when done
    defer listener.Close()
    go http.Serve(listener, nil)
    select{}
}

func start_File_client(ip_address string, command string, file_name string){

    // flag.StringVar(&ip_address, "i", ":8080", "ip address")
    // flag.StringVar(&command, "c", "grep go server.go", "message")
    // flag.Parse()
    conn, err := rpc.DialHTTP("tcp",ip_address)
    assert(err, "error to dial")
    defer conn.Close()
    args := command
    var reply string
    err = conn.Call("Arith.Command_grep", &args, &reply)
    assert(err, "Error")

    Write_2_File(reply, file_name)
}

// Helper function
func Write_2_File(s string, path string){
        var b []byte
        b = []byte(s)
        err := ioutil.WriteFile(path, b, 0644)
        if err != nil {
                log.Fatalln(err)
        }
}

// Assert Error
func assert(err error, output string){
        if err != nil {
                fmt.Println(output)
                log.Fatalln(err)
        }
}

func (t *Arith) Command_grep (command *string, reply *string) error{
    var out bytes.Buffer
    cmd := exec.Command("/bin/bash","-c",*command)
    cmd.Stdout = &out
    err := cmd.Run()
    // *reply = out.String()
    if err != nil {
        //log.Fatalln(err)
        *reply = "No matching result"
    }else{
    	*reply = out.String()
    }
    return nil
} 


func get_ip() string {
  var err error
  ls := exec.Command("hostname", "-i")
  // grep.Stdin, err = ls.StdoutPipe()
  out, err := ls.Output()
  if err != nil {//if there is no matching result
  //    fmt.Println("No matching result")
      return "Fail to fetch ip_address"
  }
  return string(out[:])
}


// Assert Error
func assert(err error, output string){
    if err != nil {
        fmt.Println(output)
        log.Fatalln(err)
    }
}