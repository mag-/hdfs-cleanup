package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

var config struct {
	namenode     string
	port         string
	prefix       string
	mtime        int
	delete       bool
	delete_limit int
}

func getImage() {
	out, err := os.Create("/tmp/image.tmp")
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()
	url := "http://" + config.namenode + ":" + config.port + "/getimage?getimage=1&txid=latest"
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		log.Fatal(err)
	}
}

func convert() {
	cmd := exec.Command("hdfs", "oiv", "-i", "/tmp/image.tmp", "-o", "/tmp/image.txt")
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func parseImage() {
	file, err := os.Open("/tmp/image.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	var count int
	count = 1
	delete_command := []string{"fs", "-rm", "-f"}

	r := bufio.NewReader(file)
	line, isPrefix, err := r.ReadLine()
	for err == nil && !isPrefix {
		s := string(line)
		fields := strings.Fields(s)
		file := fields[7]
		//only consider files with specified prefix
		if !strings.HasPrefix(fields[0], "d") && strings.HasPrefix(file, config.prefix) {
			t0, _ := time.Parse("2006-01-02", fields[5])
			t1 := time.Now()
			if t1.Sub(t0).Hours() > float64(config.mtime) {
				count = count + 1
				if config.delete {
					if count%config.delete_limit == 0 {
						deleteFiles(delete_command)
						delete_command = []string{"fs", "-rm", "-f"}
					} else {
						delete_command = append(delete_command, file)
					}
				} else {
					fmt.Print(file + "\n")
				}
			}
		}
		line, isPrefix, err = r.ReadLine()
	}
	if isPrefix {
		log.Fatal("buffer size to small")
	}
	if err != io.EOF {
		log.Fatal(err)
	}
	if config.delete {
		deleteFiles(delete_command)
	}
}

func deleteFiles(delete_command []string) {
	cmd := exec.Command("hadoop", delete_command...)
	log.Print(cmd)
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func cleanup(){
	os.Remove("/tmp/image.tmp")
	os.Remove("/tmp/image.txt")
}

func main() {
	flag.StringVar(&config.namenode, "namenode", "localhost", "namenode address")
	flag.StringVar(&config.port, "port", "50070", "namenode port")
	flag.StringVar(&config.prefix, "prefix", "/tmp", "prefix to cleanup")
	flag.IntVar(&config.mtime, "mtime", 720, "mtime of files to delete in hours")
	flag.BoolVar(&config.delete, "delete", false, "delete or just print files to delete to STDOUT")
	flag.IntVar(&config.delete_limit, "delete_limit", 1000, "delete [delete_limit] files at once")
	flag.Parse()
	log.Print("Getting image")
	getImage()
	log.Print("Converting")
	convert()
	log.Print("Processing files")
	parseImage()
	log.Print("Cleaning up")
	cleanup()
}
