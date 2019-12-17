package main

import (
	// "errors"
	"net"
	"log"
	"net/http"
	"net/rpc"
	"fmt"
	"strings"
	"os/exec"
	"./my_grep"
)

func main() {
	grep := new(my_grep.Grep)
	rpc.Register(grep)
	rpc.HandleHTTP()
	my_ip:= get_ip()
	my_ip = strings.TrimSuffix(my_ip, "\n")
	fmt.Println(my_ip + ":8080")
	l, e := net.Listen("tcp", my_ip + ":8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	for {
  	  http.Serve(l, nil)
  	}
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
