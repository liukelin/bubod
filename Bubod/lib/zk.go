package lib

import (
	"github.com/samuel/go-zookeeper/zk"
	"errors"
	"time"
	"fmt"
	"log"
	"path"
	"os"
)

type ZookeeperConfig struct {
	Servers    []string
	RootPath   string	// 集群node flag=0 并且保存同步位点信息
	MasterPath string	// 节点node命名前缀 flag=1 抢占节点，注册成功为master
}

type ElectionManager struct {
	ZKClientConn *zk.Conn
	ZKConfig     *ZookeeperConfig
	IsMaster    chan bool	// 是否选举master成功
	IsError     chan bool	// 监听状态 是否异常, 用于监听zk状态是否正常
	MyPath		string		// master节点path (冗余)
}

func NewElectionManager(zkConfig *ZookeeperConfig, isMaster chan bool, isError chan bool) *ElectionManager {
	electionManager := &ElectionManager{
		ZKClientConn: 	nil,
		ZKConfig:		zkConfig,
		IsMaster:		isMaster,
		IsError:		isError,
		MyPath:			"",
	}
	err := electionManager.initConnection()
	if err != nil {
		// 停止
		fmt.Println("zookeeper initConnection error:", err)
		os.Exit(1)
	}
	return electionManager
}

func (electionManager *ElectionManager) Run() {
	defer func() {
		electionManager.ZKClientConn.Close()
		electionManager.IsMaster <- false
	}()
	 
	err := electionManager.electMaster()
	if err != nil {
		log.Println("elect master error, ", err)
	}
	electionManager.watchMaster()
}

// 判断是否成功连接到zookeeper
func (electionManager *ElectionManager) isConnected() bool {
	if electionManager.ZKClientConn == nil || electionManager.ZKClientConn.State() != zk.StateConnected {
		return false
	}
	return true
}

// 初始化zookeeper连接
func (electionManager *ElectionManager) initConnection() error {
	// 连接为空，或连接不成功，获取zookeeper服务器的连接
	if !electionManager.isConnected() {

		conn, connChan, err := zk.Connect(electionManager.ZKConfig.Servers, time.Second)
		if err != nil {
			return err
		}

		// 等待连接成功
		for {
			isConnected := false
			select {
			case connEvent := <-connChan:
				// 等待连接
				if connEvent.State == zk.StateConnected {
					isConnected = true
					log.Println("connect to zookeeper server success.")
				}
			case _ = <-time.After(time.Second * 3): // 3秒仍未连接成功则返回连接超时
				return errors.New("connect to zookeeper server timeout.")
			}
			if isConnected {
				break
			}
		}
		electionManager.ZKClientConn = conn
	}
	return nil
}

// 注册节点，选举master
func (electionManager *ElectionManager) electMaster() error {
	err := electionManager.initConnection()
	if err != nil {
		return err
	}
	// 判断zookeeper中是否存在父节点目录，不存在则创建该目录
	isExist, _, err := electionManager.ZKClientConn.Exists(electionManager.ZKConfig.RootPath)
	if err != nil {
		return err
	}
	if !isExist {
		// var data = []byte("hello,this is a zk go test demo!!!")
		node_path, err := electionManager.ZKClientConn.Create(electionManager.ZKConfig.RootPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		if electionManager.ZKConfig.RootPath != node_path {
			return errors.New("Create returned different root path  " + electionManager.ZKConfig.RootPath + " != " + node_path)
		}
	}

	//flags有4种取值：
	//0:永久，除非手动删除
	//zk.FlagEphemeral = 1:短暂，session断开则改节点也被删除
	//zk.FlagSequence  = 2:会自动在节点后面添加序号
	//3:Ephemeral和Sequence，即，短暂且自动添加序号

	// 方法1：创建 3:Ephemeral和Sequence 类型节点，编号最小为master
	// 方法2：创建用于选举master的ZNode，该节点为Ephemeral类型（锁），创建成功则为master
	// 客户端连接断开后，其创建的节点也会被销毁
	masterPath := electionManager.ZKConfig.RootPath + electionManager.ZKConfig.MasterPath
	node_path, err := electionManager.ZKClientConn.Create(masterPath, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == nil { // 创建成功表示选举master成功
		electionManager.MyPath = path.Base(node_path)

		// fmt.Println("============path:", node_path)
		// 即刻获取当前所有节点，用于判断当前注册node是否最小节点(此步可省略，交由watchMaster统一处理)
		// 因为首次事件还未删除上一次节点，所以不生效
		// children, _, err := electionManager.ZKClientConn.Children(electionManager.ZKConfig.RootPath)
		// if err != nil {
		// 	fmt.Println(err)
		// }else{
		// 	fmt.Printf("children: %v \n", children)
		// }

		// 判断是否抢占到锁
		if node_path == masterPath {
			log.Println("zk elect master success.")
			electionManager.IsMaster <- true
		} else {
			electionManager.IsMaster <- false
			return errors.New("Create returned different path " + masterPath + " != " + node_path)
		}

	} else { // 创建失败表示选举master失败
		log.Println("zk elect master failure: ", err)
		electionManager.IsMaster <- false
		return errors.New("zk elect master failure")
	}
	return nil
}

// 监听zookeeper中master znode，表示master可能有变更，发起重新选举
func (electionManager *ElectionManager) watchMaster() {
	
	for {
		// watch zk根znode下面的子znode，当有连接断开时，对应znode被删除，触发事件后进行逻辑操作
		_, _, childCh, err := electionManager.ZKClientConn.ChildrenW(electionManager.ZKConfig.RootPath + electionManager.ZKConfig.MasterPath)
		// children, state, childCh, err := electionManager.ZKClientConn.ChildrenW(electionManager.ZKConfig.RootPath)
		if err != nil {
			electionManager.initConnection()
			log.Println("watch children error, ", err)
		}
		// log.Println("watch children result, ", children, state)
		select {
		case childEvent := <-childCh:
			// log.Println("[info]================", childEvent.Type, childEvent.Path)
			// 方法1：判断当前自己是否为最小编号节点，是则为master，可以减少重新注册消耗
			// 方法2：如果是删除node动作。则重新发起选举
			if childEvent.Type == zk.EventNodeDeleted {
			// if childEvent.Type == zk.EventNodeChildrenChanged {
				// fmt.Println("receive znode delete event, ", childEvent)

				log.Println("[info] start elect new master ...")
				/*
				// 判断最小编号节点是否自己
				check := true
				for _, p := range children {
					if p < electionManager.MyPath {
						check = false
					}
				}
				electionManager.IsMaster <- check

				if check {
					log.Println("[info] elect master success!")
				}*/

				// 重新选举
				err = electionManager.electMaster()
				if err != nil {
					log.Println("elect new master error: ", err)
					electionManager.IsError <- false // 表示
				}

			}
		}
		// time.Sleep(1 * time.Second)

	}

}

// 修改节点内容
func (electionManager *ElectionManager) SetData(new_data string) error {
	isExist, s, err := electionManager.ZKClientConn.Exists(electionManager.ZKConfig.RootPath)
	if err != nil {
		return err
	}
	if !isExist {
		return nil
	}
	// update
	// var new_data = []byte("zk_test_new_value")
	s, err = electionManager.ZKClientConn.Set(electionManager.ZKConfig.RootPath, []byte(new_data), s.Version)
	if err != nil {
		log.Println("zk Set data error: ", err)
		return err
	}
	return nil
}

// 获取节点内容
func (electionManager *ElectionManager) GetData() string{
	// get
	v, _, err := electionManager.ZKClientConn.Get(electionManager.ZKConfig.RootPath)
	if err != nil {
		log.Println("zk Get data error: ", err)
		return ""
	}
	return string(v[:])
}



func main() {
	// zookeeper配置
	zkConfig := &ZookeeperConfig{
		Servers:    []string{"127.0.0.1:8121"},
		RootPath:   "/ElectMasterDemo",
		MasterPath: "/master",
	}
	// main goroutine 和 选举goroutine之间通信的channel，同于返回选角结果
	isMasterChan := make(chan bool)
	isErrorChan := make(chan bool)

	var isMaster bool
	// var isError  bool

	// 选举
	electionManager := NewElectionManager(zkConfig, isMasterChan, isErrorChan)
	go electionManager.Run()

	for {
		select {
		case isMaster = <-isMasterChan:
			if isMaster {
				// do some job on master
				fmt.Println("do some job on master")
			}
		}
	}
}