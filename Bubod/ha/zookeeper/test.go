// zookeeper 相关
// 使用临时节点，作为服务注册。编号最小的节点为start，其余为backup
package zookeeper

import (
    "fmt"
    "github.com/samuel/go-zookeeper/zk"
    "time"
)

type zkConn struct {
    hosts           string
	zkConn          *zk.Conn
}

// 连接
func (zkConn *zkConn) ZkConntion() (conn *zk.Conn){
    
    return nil
}

// 注册节点

// func  

func ZkStart() {
    var hosts = []string{"localhost:2181"}
    var path1 = "/test_zk"
    // var flags int32 = zk.FlagEphemeral
    var data1 = []byte("hello,this is a zk go test demo!!!")
    // var acls = zk.WorldACL(zk.PermAll)

    option := zk.WithEventCallback(callback)

    conn, _, err := zk.Connect(hosts, time.Second*5, option)
    if err != nil {
        fmt.Println(err)
        return
    }

    defer conn.Close()

    _, _, _, err = conn.ExistsW(path1)
    if err != nil {
        fmt.Println(err)
        return
    }

    create(conn, path1, data1)
    time.Sleep(time.Second * 2)
    _, _, _, err = conn.ExistsW(path1)
    if err != nil {
        fmt.Println(err)
        return
    }
    // delete(conn, path1)
}

func create(conn *zk.Conn, path string, data []byte) {
    var flags int32 = zk.FlagEphemeral
    var acls = zk.WorldACL(zk.PermAll)

    _, err_create := conn.Create(path, data, flags, acls)
    if err_create != nil {
        fmt.Println(err_create)
        return
    }

}

func callback(event zk.Event) {
    fmt.Println("*******************")
    fmt.Println("path:", event.Path)
    fmt.Println("type:", event.Type.String())
    fmt.Println("state:", event.State.String())
    fmt.Println("-------------------")
}
