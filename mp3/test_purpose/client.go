package main

import (
        "fmt"
        "log"
        "flag"
        "net/rpc"
        "io/ioutil"
)

func main(){
	fmt.Println("Grep request Client")

    var ip_address string
    var command string

    flag.StringVar(&ip_address, "i", ":8080", "ip address")
    flag.StringVar(&command, "c", "grep go server.go", "message")
    flag.Parse()

    conn, err := rpc.DialHTTP("tcp",ip_address)
    assert(err, "error to dial")
    defer conn.Close()

    // change sdfsname to localname
    sdfsname := command
    
    args := command
    var reply string
    err = conn.Call("Arith.Command_grep", &args, &reply)
    assert(err, "Error")

    Write_2_File(reply, "grep_result.txt")
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