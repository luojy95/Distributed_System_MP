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
	var lines int
	var pattern1 int
	var pattern2 int
	var pattern3 int
	var pattern4 int
	var frequent1 int
	var frequent2 int
	var frequent3 int
	var frequent4 int
	var name string
	flag.IntVar(&lines, "l", 1000000, "total lines")
	flag.IntVar(&pattern1, "l1", 1234567890, "line count for pattern1")
	flag.IntVar(&pattern2, "l2", 1357913579, "line count for pattern2")
	flag.IntVar(&pattern3, "l3", 2468024680, "line count for pattern3")
	flag.IntVar(&pattern4, "l4", 1470369258, "line count for pattern4")
	flag.IntVar(&frequent1, "f1", 1, "frequent for pattern1")
	flag.IntVar(&frequent2, "f2", 500, "frequent for pattern2")
	flag.IntVar(&frequent3, "f3", 10000, "frequent for pattern3")
	flag.IntVar(&frequent4, "f4", 500000, "frequent for pattern4")
	flag.StringVar(&name, "n", "log_all_patterns.log", "filename")
	flag.Parse()
	file, e := os.OpenFile(name, os.O_CREATE| os.O_RDWR, 0644)
	if e!=nil{
		log.Fatalln("failed")
	}

	for i := 0; i < frequent1; i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(pattern1)
		log.SetOutput(file)
		log.Println(rand_part1+const_part+rand_part2)
	}

	for i := 0; i < frequent2; i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(pattern2)
		log.SetOutput(file)
		log.Println(rand_part1+const_part+rand_part2)
	}

	for i := 0; i < frequent3; i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(pattern3)
		log.SetOutput(file)
		log.Println(rand_part1+const_part+rand_part2)
	}

	for i := 0; i < frequent4; i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(pattern4)
		log.SetOutput(file)
		log.Println(rand_part1+const_part+rand_part2)
	}

	for i := 0; i < (lines - frequent4 - frequent3 - frequent2 - frequent1); i++{
		index1 := generate_random(a,b,c)
		index2 := generate_random(a,b,c)
		rand_part1 := strconv.Itoa(rand.Intn(index1))
		rand_part2 := strconv.Itoa(rand.Intn(index2))
		const_part := strconv.Itoa(rand.Intn(pattern1*6))
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