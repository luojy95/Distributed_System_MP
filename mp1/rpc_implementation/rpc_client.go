package main
import (
        "log"
	"os"
        "fmt"
        "net/rpc"
        "flag"
	"strconv"
	"strings"
	"io/ioutil"
	"time"
)

func main() {
        //ip address
        var ip_ad string
        
	//grep command to execute
    	var request string

	//grep options
	var options string
	
	//path to save result
        var path string
	
	//print to terminal or not, "y" will print, any other won't
	var wprint string

	//directory and file name of file to grep
	var filename string

	//suffix of file name
	var suffix string

	//result of saved file name
	var save_name string

        flag.StringVar(&request, "c", "grep -c 0", "message");
        flag.StringVar(&options, "a", " -c ", "options");
	flag.StringVar(&path, "p", "./result", "path");
	flag.StringVar(&wprint, "o", "y", "print_to_terminal");
	flag.StringVar(&filename, "f", "~/logs/\"random log file\"/log", "prefix of file name");
	//flag.StringVar(&filename, "f", "~/logs/vm", "prefix of file name");
	flag.StringVar(&suffix, "s", ".log", "suffix");
	flag.StringVar(&save_name, "sf", "result_vm", "result of saved file name")
	flag.Parse()

	vm_idx_to_ip := map[int]string{
 		1: "172.22.154.82:8080",
		2: "172.22.156.82:8080",
		3: "172.22.158.82:8080",
		4: "172.22.154.83:8080",
		5: "172.22.156.83:8080",
		6: "172.22.158.83:8080",
		7: "172.22.154.84:8080",
		8: "172.22.156.84:8080",
		9: "172.22.158.84:8080",
		10: "172.22.154.85:8080",
	}	
 	start:= time.Now()	
	var vm_num string
	var commandct, command string
	total_count := 0;
	for i := 1; i <= 10; i++ {
		ip_ad = vm_idx_to_ip[i]
		client, err := rpc.DialHTTP("tcp", ip_ad)
       		//defer client.Close()
		vm_num = "vm-" + strconv.Itoa(i)
		if err != nil {
                        //log.Fatal("grep error:", err)
                        fmt.Println("========================================")
			fmt.Println("Fail to dial " + vm_num)
                } else { 
			fmt.Println("========================================\nConnected to vm " + vm_num)
	//		vm_check(err, vm_num)
        		var result_count string
			commandct = request + " -c " + filename + strconv.Itoa(i) + suffix
			err = client.Call("Grep.Grep_info", &commandct, &result_count)
			fmt.Print("Result count:" + result_count)
			if s, err:= strconv.Atoi(strings.TrimSuffix(result_count, "\n"));err == nil {
				total_count += s;
			}
			var reply string
			command = request + " " + options + " " + filename + strconv.Itoa(i) + suffix
       			err = client.Call("Grep.Grep_info", &command, &reply)
                	if (wprint == "y") {//print to terminal
				fmt.Println("Reply:" + reply)
			} else {//save to file
				fmt.Println("Reply saved to " + path + "/" + save_name + strconv.Itoa(i) + ".txt")
				Write_2_File(reply, path, "/" + save_name + strconv.Itoa(i) + ".txt")
			}
		}	
	}
	fmt.Println("\n==================== Summary  ===========================\n")
	fmt.Println("Total count:", total_count);
	end:= time.Now()
	t:= end.Sub(start)
	fmt.Println("Time: ", t)
}	



func Write_2_File(s string, path string, filename string){
        if _, err := os.Stat(path); os.IsNotExist(err) {
    		os.Mkdir(path, 0700)
	}		
	var b []byte
        b = []byte(s)
        err := ioutil.WriteFile(path + filename, b, 0644)
        if err != nil {
                log.Fatalln(err)
        }
}


// Assert
func assert(err error, msg string) {
  if err != nil {
    fmt.Println(msg)
    log.Fatalln(err)
  }
}
//func vm_check(err error, vm_num string) {
//  if err != nil {
//    fmt.Println("error to dial: ", vm_num)
    //log.Fatalln(err)
//  } else {
//    fmt.Println("start running client: ", vm_num)
    //get_response(conn, command)
//  }
//}
