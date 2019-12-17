package main

import (
	"log"
	"os"
	"math/rand"
	"strconv"
	"flag"
	"time"
	//"fmt"
	// "strings"
)

func main(){
	now := time.Now()
	a,b,c := now.Clock()
	// fmt.Println(a[0])
	var num_const int
	var num_line int
	var const_num int
	var name string
	flag.IntVar(&num_const, "c", 100, "constant line")
	flag.IntVar(&num_line, "l", 1000, "random line")
	flag.IntVar(&const_num, "v", 123456789, "const value")
	flag.StringVar(&name, "n", "log11.log", "filename")
	flag.Parse()
	file, e := os.OpenFile(name, os.O_CREATE| os.O_RDWR, 0644)
	if e!=nil{
		log.Fatalln("failed")
	}

	for i := 0; i < num_const; i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(const_num)
		log.SetOutput(file)
		log.Println(rand_part1+const_part+rand_part2)
	}

	for i := 0; i < num_line - num_const; i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(rand.Intn(const_num*6))
		log.SetOutput(file)
		log.Println(rand_part1+const_part+rand_part2)
	}

	// log.SetOutput(file)
	// log.Println("123")
	// log.Println("234")
	// log.Println("456")
	// a := strconv.Itoa(rand.Intn(1000000000000000000)) + "123123"
	// // A := string(a)
	// b := rand.Intn(1000000000000000000)
	// c := rand.Intn(1000000000000000000)
	// log.Println(a)
	// log.Println(b)
	// log.Println(c)
}

func generate_random(a int, b int, c int) int{
	return rand.Intn(1000000000000*a*b*c)
}

