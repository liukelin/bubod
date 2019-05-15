//同步信息
package lib
import(
	"os"
	"io"
	"log"
	"fmt"
	"time"
)

/**
  提供每秒同步一次的服务
*/
func (dumpConfig *DumpConfig) InstantSync() {
	
	// 持续尝试写入
	for {
		// 判断是否有变更(可忽略)
		newPos := fmt.Sprintf("%s:%d" ,dumpConfig.BinlogDumpFileName, dumpConfig.BinlogDumpPosition)
		if newPos != dumpConfig.SyncPos {
			// 保存同步
			dumpConfig.SyncBinlogFilenamePos(newPos)
			log.Println("Sync BinlogFilenamePos=============:", dumpConfig.SyncPos, " to " , newPos)
		}
		time.Sleep(1 * time.Second)
	}
}

/*
  保存当前pos信息
	1、将当前同步位点信息同步到 file/zookeeper
	2、同步到zookeeper 是作为高可用环境下服务接力。考虑到zookeeper读写性能差。这里可使用其他公共存储组件(如redis)
	3、fileNamePos  file_name:position
*/
func (dumpConfig *DumpConfig) SyncBinlogFilenamePos(fileNamePos string) error {

	if CheckBinlogFilePos(fileNamePos) == false {
		return fmt.Errorf("[error] SyncBinlogFilenamePos Invalid fileNamePos error %s", fileNamePos)
	}

	// bubod_dump_pos := config.GetConfigVal("Bubod","bubod_dump_pos")
	bubod_dump_pos := dumpConfig.Conf["Bubod"]["bubod_dump_pos"]

	f, err := os.OpenFile(bubod_dump_pos, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777) //打开文件
	if err !=nil {
		log.Println("[error] Write bubod_dump_pos OpenFile error:", bubod_dump_pos, "; Error:",err)
		return err
	}else{
		_, err := io.WriteString(f, fileNamePos)
		if err !=nil {
			log.Println("[error] Write bubod_dump_pos WriteString err:", bubod_dump_pos, "; Error:",err)
			return err
		}
	}	
	defer f.Close()

	// 同步到zk
	if (dumpConfig.Conf["Zookeeper"]["server"] != ""){
		dumpConfig.Ha.ZkClient.SetData(fileNamePos)
	}
	// 已同步位点
	dumpConfig.SyncPos = fileNamePos

	return nil
}