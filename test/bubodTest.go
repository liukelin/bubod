package main
import (
	// "sync"
	"fmt"
	"bubod/Bubod/lib"
	// "log"
	// "time"
)

func doStart() {
	fmt.Printf("doStart...\n")

	dumpConfig := &lib.DumpConfig{
		ConnectUri:			"root:@tcp(127.0.0.1:3306)/bifrost_test",
		ClusterName:		"test",
		NodeName:			"test",
		ServerId:			2,
		TableMap:			make(map[string]*lib.Table, 0),
		BinlogDumpFileName:	"mysql-bin.000003",
		BinlogDumpPosition:	120,
	}
	dumpConfig.AddDump().Start()
	
	fmt.Printf("do end...\n")
}

func main(){
	doStart()
}