package main
import(
// "os"
// "strings"
// "log"
"bytes"
"fmt"
"os/exec"
"strings"
)

// func find_file(file_list []string, target string)(ret []string){
// 	ret = []string
// 	for i := 0; i < len(file_list) - 1; i++{
// 		exist := strings.Contains(file_list[i], target)
// 		if(exist){
// 			ret.append(file_list[i])
// 		}
// 	}
// 	return ret
// }

func main(){
    // var out bytes.Buffer
    // var out bytes.Buffer
    var out_file1 bytes.Buffer
    var out_file bytes.Buffer
    // file_list := exec.Command("/bin/bash","-c", "find . -name \"a_*\"")
    // file_list.Stdout = &out_file
    // file_list.Run()
    // // fmt.Println(out_file.String())
    // ret := strings.Split(out_file.String(), "\n")
    // fmt.Println(ret)
    find_cmd1 := "cd ./local_dir && mkdir jiangzhoutongniubibi"
    file_cmd1 := exec.Command("/bin/bash","-c", find_cmd1)
    file_cmd1.Stdout = &out_file1
    file_cmd1.Run()

    find_cmd := "cd ./local_dir && mkdir jiangzhoutongniubi"
    file_cmd := exec.Command("/bin/bash","-c", find_cmd)
    file_cmd.Stdout = &out_file
    file_cmd.Run()
    ret := strings.Split(out_file.String(), "\n")
    // file_latest := out_file.String()//[len(out_file.String()) - 1]
    fmt.Println(out_file.String())
    fmt.Println(ret)
    fmt.Println(len(ret))
    // fmt.Println(ret[len(ret) - 2])
    // cat_cmd := "cat " + file_latest
    // cmd := exec.Command("/bin/bash","-c",cat_cmd)
    // cmd.Stdout = &out
    // err := cmd.Run()
    // cmd := exec.Command("/bin/bash","-c",*command)
    // cmd.Stdout = &out
    // err := cmd.Run()
    // *reply = out.String()
    // if err != nil {
    //     log.Fatalln(err)
    //     *reply = "No matching result"
    // }
    // else{
    	// *reply = out.String()
    // }
    return
}

