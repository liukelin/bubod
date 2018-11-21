package lib

import (
	"bubod/Bubod/mysql"
	"log"
	// "fmt"
)

// 数据接收回调函数
// 1、将数据写入mq
// 2、更新同步位点信息 file/zookeeper
func (dump *dump) Callback(data *mysql.EventReslut) {
	if len(data.Rows) == 0 {
		return
	}
	// key := data.SchemaName + "-" + data.TableName
	// fmt.Printf(key)

	// log.Println("===Callback:")
	// log.Println("===EventHeader:", data.Header)
	// log.Println("===EventType:", mysql.EvenTypeName(data.Header.EventType))
	// log.Println("===SchemaName:", data.SchemaName)
	// log.Println("===TableName:", data.TableName)
	// log.Println("===Query:", data.Query)
	// log.Println("===BinlogFileName:", data.BinlogFileName)
	// log.Println("===BinlogPosition:", data.BinlogPosition)
	// log.Println("===Rows:", data)
	jsonDatas := mysql.FormatEventData(data)

	if dump.dumpConfig.Conf["Bubod"]["debug"] == "true" {
		log.Println(jsonDatas)
	}
	
	dump.dumpConfig.BinlogDumpFileName = data.BinlogFileName
	dump.dumpConfig.BinlogDumpPosition = data.BinlogPosition
	// go func(){ 
		// 这里交给异步服务做
		// sync pos
		// filepos	:= fmt.Sprintf("%s:%d", data.BinlogFileName, data.BinlogPosition)
		// err := dump.dumpConfig.SyncBinlogFilenamePos(filepos)
		// if err != nil {
		// 	log.Println("[error] Callback SyncBinlogFilenamePos error:", err)
		// }
	// }()
	
	go func(){
		// push mq
		for _,v := range jsonDatas {
			if dump.dumpConfig.MqClass != nil {
				err_ := dump.dumpConfig.MqClass.Push(v)
				if err_ != nil {
					log.Println("[error] push error:", err_)
				}
			}
		}
	}()

	return
}