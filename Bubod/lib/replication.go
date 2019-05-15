package lib
import (
	"sync"
	"log"
	"time"
	"fmt"
	"strconv"
	"strings"
	"bubod/Bubod/config"
	"bubod/Bubod/mysql"
	"bubod/Bubod/mq/disque"
	"bubod/Bubod/mq/rabbit"
	zk "bubod/Bubod/ha/zookeeper"
	// "bubod/Bubod/ha/etcd"
)

// 配置属性
type DumpConfig struct {
	ConnectUri 				string `json:"ConnectUri"`
	ClusterName				string `json:"ClusterName"`				 
	NodeName               	string `json:"NodeName"`				 // 服务名称
	ServerId				uint32 `json:"ServerId"`				 // 节点唯一
	TableMap           		map[string]*Table `json:"TableMap"` 	 // 用于缓存 需要同步的表 tableName=>*Table
	FilterTableMap          map[string]*Table `json:"FilterTableMap"`// 用于缓存 屏蔽同步表
	BinlogDumpFileName 		string `json:"BinlogDumpFileName"`		 // 需要注意的问题是一个binlog事件占几行，起始位置需要正确，否则解析失败
	BinlogDumpPosition 		uint32 `json:"BinlogDumpPosition"`		 // pos
	Conf					map[string]map[string]string 			 // 所有配置
	MqClass 				MqClass									 // mq
	SyncPos					string									 // 已同步位点。（同步当前pos信息） 同时读写可能有锁问题
	Ha 						*Ha										 // 高可用相关配置
}

// 高可用相关， 目前支持zk/etcd 后续可改为统一 interface
type Ha struct {
	ZkClient 	*zk.ElectionManager // zookeeper
	// etcdClient	
	ErrorChan	chan bool		 // 运行状态
	MasterChan	chan bool		 // 是否master
}

// table
type Table struct {
	sync.Mutex
	Name         string		// 表名
	SyncStatus	 bool		// 是否需要同步 是 否
	// ChannelKey   int		// 使用的队列（弃用）
}

// binlog dump必要参数
type dump struct {
	sync.Mutex
	ConnStatus         		string //close,stop,starting,running
	ConnErr            		string 
	binlogDump         		*mysql.BinlogDump
	replicateDoDb     	 	map[string]uint8
	killStatus 			  	int
	// maxBinlogDumpFileName 	string
	// maxBinlogDumpPosition 	uint32
	dumpConfig				*DumpConfig
}

//队列类实现方法
type MqClass interface {
	Connect() (error)
	Push(string) (error)
}

func Run(conf map[string]map[string]string){

	database := config.GetConf("Database")
	connectUri := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", database["user"], database["pass"], database["host"], database["port"], database["db"] ) 
	server_id, err := strconv.ParseUint(database["server_id"], 10, 64)
	if err != nil {
		log.Println("[error] config file server_id error:", err)
		return
	}

	dumpConfig := &DumpConfig{
		ClusterName:		config.GetConfigVal("Bubod","cluster_name"),
		NodeName:			config.GetConfigVal("Bubod","node_name"),
		ConnectUri:			connectUri,
		ServerId:			uint32(server_id),
		TableMap:			make(map[string]*Table, 0),
		FilterTableMap:		make(map[string]*Table, 0),
		BinlogDumpFileName:	"", //"mysql-bin.000003",
		BinlogDumpPosition:	0,  // 120,
		Conf:				conf,
		Ha:					&Ha{
								ZkClient: 	nil,
								ErrorChan:	make(chan bool),
								MasterChan:	make(chan bool),
							},
	}

	// 高可用环境下 注册服务
	if (config.GetConfigVal("Zookeeper","server") != ""){
		zkserv := strings.Split(config.GetConfigVal("Zookeeper","server"), ",")

		// zookeeper配置
		zkConfig := &zk.Config{
			Servers:    zkserv, // []string{"127.0.0.1:2181"},
			RootPath:   "/"+dumpConfig.ClusterName,
			MasterPath: "/master",
		}

		// 选举
		dumpConfig.Ha.ZkClient = zk.NewElectionManager(zkConfig, dumpConfig.Ha.MasterChan, dumpConfig.Ha.ErrorChan)
		go dumpConfig.Ha.ZkClient.Run()

		log.Println("Ha elect master Waiting ...")

		for {
			select {
			case isMaster := <-dumpConfig.Ha.MasterChan:
				if isMaster {
					log.Println("Ha elect master success.")
					break
				}
			}
			time.Sleep(1 * time.Second)
		}
	}

	// 初始化消息队列
	// var mqClass MqClass
	qname := config.GetConfigVal("Channel","qname")
	switch (config.GetConfigVal("Channel","type")) {
		case "disque":
			mqServers := strings.Split(config.GetConfigVal("Channel","servers"), ",")
			dumpConfig.MqClass = &disque.Mq{
				MqConf: &disque.MqConf{
					Servers: mqServers,
					Qname: qname,
				},
				Pool:	nil,
				Conn: 	nil,	
			}
		case "rabbit":
			dumpConfig.MqClass = &rabbit.Mq{
				Amqp:	config.GetConfigVal("Channel","amqp"),
				Qname:	qname,
				Conn:	nil,
				Channel:nil,
			}
		default:
			log.Println("[error] Channel type error .")
			return
	}
	err_ := dumpConfig.MqClass.Connect()
	if err_ != nil {
		log.Println("[error] Connect mq error .", err_)
		return 
	}

	// 获取最新位点
	dumpConfig.GetLastPosition()

	// 启动sync 位点同步服务
	go dumpConfig.InstantSync()

	// started...
	dumpConfig.AddDump().Start()
}

// 参数生成配置
func (dumpConfig *DumpConfig)AddDump() *dump{
	
	// var binlogDump *mysql.BinlogDump
	binlogDump := &mysql.BinlogDump{
		DataSource:    dumpConfig.ConnectUri,
		ReplicateDoDb: make(map[string]uint8, 0),
		OnlyEvent:     []mysql.EventType{
							mysql.WRITE_ROWS_EVENTv1, 
							mysql.UPDATE_ROWS_EVENTv1, 
							mysql.DELETE_ROWS_EVENTv1,
							mysql.WRITE_ROWS_EVENTv0, 
							mysql.UPDATE_ROWS_EVENTv0, 
							mysql.DELETE_ROWS_EVENTv0,
							mysql.WRITE_ROWS_EVENTv2, 
							mysql.UPDATE_ROWS_EVENTv2, 
							mysql.DELETE_ROWS_EVENTv2,
							mysql.QUERY_EVENT},
	}

	return &dump{
		ConnStatus:     "starting",
		ConnErr:        "starting", 
		binlogDump: 	binlogDump,
		replicateDoDb: 	make(map[string]uint8, 0),
		killStatus:		0,
		dumpConfig:		dumpConfig,
		// maxBinlogDumpFileName:	"",	 // 事件最大限制
		// maxBinlogDumpPosition:	0,	 // 事件最大限制
	}
}

// 启动同步
func (dump *dump) Start() {

	// 接收消息信号量
	reslut := make(chan error, 1)
	dump.binlogDump.CallbackFun = dump.Callback
	
	go dump.binlogDump.StartDumpBinlog(dump.dumpConfig.BinlogDumpFileName, dump.dumpConfig.BinlogDumpPosition, dump.dumpConfig.ServerId, reslut, "", 0)

	// 持续消费输出信息
	// 主进程阻塞
	for {
		select {
		case msg := <-reslut:
			log.Printf("reslut:%s \r\n", msg)
			
		case zkStatus := <-dump.dumpConfig.Ha.ErrorChan:
			if !zkStatus{
				// 如果高可用模式下zk 连接发送故障 则终止服务
				log.Printf("ZkErrorChan error\r\n")
				break
			}
		case isMaster := <-dump.dumpConfig.Ha.MasterChan:
			if !isMaster{
				// 如果高可用模式下zk失去master 则终止服务
				log.Printf("ZkMasterChan error\r\n")
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

}