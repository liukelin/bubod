package main
import (
	"bubod/Bubod/lib"
	"fmt"
	"log"
)

func main() {
	// zookeeper配置
	zkConfig := &lib.ZookeeperConfig{
		Servers:    []string{"127.0.0.1:2181"},
		RootPath:   "/ElectMasterDemo",
		MasterPath: "/master",
	}
	// main goroutine 和 选举goroutine之间通信的channel，同于返回选角结果
	isMasterChan := make(chan bool)
	isErrorChan := make(chan bool)

	var isMaster bool
	var isError  bool

	// 选举
	electionManager := lib.NewElectionManager(zkConfig, isMasterChan, isErrorChan)
	go electionManager.Run()

	electionManager.SetData("zk_test_new_value")
	a :=electionManager.GetData()
	log.Println("================:", a)

	for {
		select {
		case isMaster = <-isMasterChan:
			if isMaster {
				// do some job on master
				fmt.Println("do some job on master.")
			}
		case isError = <-isErrorChan:
			if isError {
				fmt.Println("zk error.")
			}

		}
	}
}