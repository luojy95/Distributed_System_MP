package my_grep

import (
	// "errors"
	// "net"
	// "log"
	// "net/http"
	"os/exec"
	//"fmt"
	// "net/rpc"
)

type Grep int

func (t *Grep) Grep_info(args *string, reply *string) error {
	*reply = cmd(*args)
	return nil
}

func cmd(command string) string {
  var err error
  // ls := exec.Command("ls", "-la")
  ls := exec.Command("/bin/sh", "-c", command)
  // grep.Stdin, err = ls.StdoutPipe()
  out, err := ls.Output()
  if err != nil {//if there is no matching result
      //fmt.Println("No matching result")
     return "No matching result\n"
  }
  return string(out[:])
}

