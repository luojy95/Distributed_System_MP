package main

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
)

// type Args struct {
// 	A string
// }
// type Args string

type Arith int

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

func main(){
	fmt.Println("Grep request Server")

    var ip_address string
    
    flag.StringVar(&ip_address, "i", "172.22.156.82:8080", "ip address")
    flag.Parse()

	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()

	// res_address, err := net.ResolveTCPAddr("tcp", ip_address)
	// assert(err, "Error 1")
    my_ip:= get_ip()
	my_ip = strings.TrimSuffix(my_ip, "\n")
    
	// Create listener
    listener, err := net.Listen("tcp", my_ip + ":8080")

    // Assert error
    assert(err, "Can not create listener")

    // Clean up listener when done
    defer listener.Close()

    go http.Serve(listener, nil)
    select{}
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