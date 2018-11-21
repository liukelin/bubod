/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"log"
	"bubod/Bubod/config"
	"bubod/Bubod/lib"
	"flag"
	"os"
	"time"
	"io"
	"sync"
	"io/ioutil"
	"fmt"
	"strings"
	"path/filepath"
	"runtime"
)

var l sync.Mutex

var Conf map[string]map[string]string
var Daemon string
var Pid string
var ConfigFile *string 

var logo = `
_           _               _ 
| |         | |             | |    bubod {$version} {$system} 
| |__  _   _| |__   ___   __| |    cluster: {$cluster}	node: {$node} 
| '_ \| | | | '_ \ / _ \ / _  |    Pid: {$Pid}
| |_) | |_| | |_) | (_) | (_| |    http://bubod.com
|_.__/ \__,_|_.__/ \___/ \__,_|
`

func printLogo(cluster string, node_name string){
	logo = strings.Replace(logo,"{$version}",config.VERSION,-1)
	logo = strings.Replace(logo,"{$cluster}",cluster,-1)
	logo = strings.Replace(logo,"{$node}",node_name,-1)
	logo = strings.Replace(logo,"{$Pid}",fmt.Sprint(os.Getpid()),-1)
	logo = strings.Replace(logo,"{$system}",fmt.Sprint(runtime.GOARCH),-1)
	fmt.Println(logo)
}

func main() {
	defer func() {
		if Pid != ""{
			os.Remove(Pid)
			log.Println("Remove Pid:", Pid, os.Getppid())
		}
	}()

	execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	ConfigFile = flag.String("config", "", "配置文件路径")
	flag.Parse()

	if *ConfigFile == "" {
		*ConfigFile = execDir+"/bubod.ini" // 默认为跟目录
		log.Println("ConfigFile:",*ConfigFile)
	}
	
	// 指向config.MyConf
	Conf = config.LoadConf(*ConfigFile)

	cluster_name := config.GetConfigVal("Bubod","cluster_name")
	server_id := config.GetConfigVal("Database","server_id")
	Daemon = config.GetConfigVal("Bubod","daemon")


	dataDir := config.GetConfigVal("Bubod","data_dir")
	if dataDir == ""{
		dataDir = execDir+"/data"
	}
	os.MkdirAll(dataDir, 0777)
	
	// 数据文件路径
	// config.SetConfigVal("Bubod","data_dir", dataDir)
	Conf["Bubod"]["data_dir"] = dataDir

	// 节点名称
	node_name := fmt.Sprintf("bubod-%s-node-%s", cluster_name, server_id)
	// config.SetConfigVal("Bubod","node_name", node_name)
	Conf["Bubod"]["node_name"] = node_name

	// 记录位点信息文件路径
	// config.SetConfigVal("Bubod","bubod_dump_pos", dataDir+"/filepos-"+node_name+".bubod")
	// config.SetConfigVal("Bubod","bubod_dump_pos_tmp", dataDir+"/filepos-"+node_name+".bubod.tmp")
	Conf["Bubod"]["bubod_dump_pos"] = dataDir+"/filepos-"+node_name+".bubod"
	Conf["Bubod"]["bubod_dump_pos_tmp"] = dataDir+"/filepos-"+node_name+".bubod.tmp"

	if Daemon == "true"{
		if os.Getppid() != 1{
			filePath,_:=filepath.Abs(os.Args[0])  //将命令行参数中执行文件路径转换成可用路径
			args:=append([]string{filePath},os.Args[1:]...)
			os.StartProcess(filePath,args,&os.ProcAttr{Files:[]*os.File{os.Stdin,os.Stdout,os.Stderr}})
			return
		}else{
			printLogo(cluster_name,node_name)
			initLog()
		}
	}else{
		printLogo(cluster_name,node_name)
	}

	if runtime.GOOS != "windows" {
		if Pid == ""{
			if config.GetConfigVal("Bubod","pid") == ""{
				Pid = dataDir+"/"+node_name+".pid"
				// config.SetConfigVal("Bubod","pid", Pid) // pid文件路径
				Conf["Bubod"]["pid"] = Pid
			}else{
				Pid = config.GetConfigVal("Bubod","pid")
			}
		}
		WritePid()
	}

	log.Println("Server started...")
	lib.Run(Conf)
}

// 记录输出日志
func initLog(){
	log_dir := config.GetConfigVal("bubod","log_dir")
	if log_dir == ""{
		log_dir, _ = filepath.Abs(filepath.Dir(os.Args[0]))
		log_dir += "/logs"
	}
	os.MkdirAll(log_dir,0777)

	// 保存进配置，log路径
	// config.SetConfigVal("bubod","log_dir", log_dir)
	Conf["Bubod"]["log_dir"] = log_dir

	t := time.Now().Format("2006-01-02")
	LogFileName := log_dir+"/bubod_"+t+".log"
	f, err := os.OpenFile(LogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777) //打开文件
	if err != nil{
		log.Println("log init error:",err)
	}
	log.SetOutput(f)
}

// 写入pid文件
func WritePid(){
	f, err2 := os.OpenFile(Pid, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777) //打开文件
	if err2 !=nil {
		log.Println("Open Pid Error; File:",Pid,"; Error:",err2)
		os.Exit(1)
		return
	}
	pidContent, err2 := ioutil.ReadAll(f)
	if string(pidContent) != "" {
		log.Println("bubod server quit without delete PID file; File:",Pid, string(pidContent),"; Error:",err2)
		os.Exit(1)
	}
	defer f.Close()
	io.WriteString(f, fmt.Sprint(os.Getpid()))
}
