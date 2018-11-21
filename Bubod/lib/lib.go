package lib

import (
	"bubod/Bubod/config"
	"os"
	"log"
	"io/ioutil"
	"strings"
	"strconv"
)


// 从conf/本地文件data_file/zk 获取最新的filename+position
// return file_name:position
func (dumpConfig *DumpConfig) GetLastPosition() string{

	var file_pos string
	var config_pos string	 // 配置文件提供的pos
	var data_file_pos string // 文件记录的pos
	var zk_pos string		 // zk上保存的pos

	config_pos = ""
	if config.GetConfigVal("Database","binlog_dump_file_name")!="" && config.GetConfigVal("Database","binlog_dump_position")!="" {
		config_pos = config.GetConfigVal("Database","binlog_dump_file_name")+":"+config.GetConfigVal("Database","binlog_dump_position")
	}
	// 从data file获取当前位点
	bubod_dump_pos := config.GetConfigVal("Bubod","bubod_dump_pos")
	// bubod_dump_pos := dumpConfig.Conf["Bubod"]["bubod_dump_pos"]

	f, err2 := os.OpenFile(bubod_dump_pos, os.O_CREATE|os.O_RDWR, 0777)
	defer f.Close()
	if err2 !=nil {
		log.Println("[info] Open bubod_dump_pos:", bubod_dump_pos,"; Error:",err2)
	}else{
		content, err2 := ioutil.ReadAll(f)
		if err2 != nil {
			log.Println("[info] Read bubod_dump_pos:", bubod_dump_pos,"; Error:",err2)
		}else{
			data_file_pos = string(content)
		}
	}

	// 从zookeeper获取位点信息，待实现。。。
	if (config.GetConfigVal("Zookeeper","server") != ""){
		zk_pos = dumpConfig.ElectionManager.GetData()
	}

	// 使用最大位点
	if config_pos != "" && CheckBinlogFilePos(config_pos) {
		file_pos = config_pos
	}
	if CheckBinlogFilePos(data_file_pos) && (file_pos == "" || file_pos < data_file_pos) {
		file_pos = data_file_pos
	}
	if CheckBinlogFilePos(zk_pos) && (file_pos =="" || file_pos < zk_pos) {
		file_pos = zk_pos
	}
	
	if CheckBinlogFilePos(file_pos){
		filepos := strings.Split(file_pos, ":")
		pos, _ := strconv.ParseUint(filepos[1], 10, 64)
		dumpConfig.BinlogDumpFileName = filepos[0]
		dumpConfig.BinlogDumpPosition = uint32(pos)
	}
	return file_pos
}

// 检测字符串是否为位点 filename:position
func CheckBinlogFilePos(file_pos string) bool {
	if file_pos != "" && strings.ContainsRune(file_pos, ':'){
		return true
	}
	return false
}
