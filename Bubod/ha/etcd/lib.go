package etcd

import (
	"time"
	"go.etcd.io/etcd/clientv3"
)

type Config struct {
	DialTimeout: 5 * time.Second,
	Servers    []string	// 服务节点列表 []string{"localhost:2379", "localhost:2380", "localhost:2381"},
	RootPath   string	// 集群node flag=0 并且保存同步位点信息
	MasterPath string	// 节点node，注册成功为master
}


// initConnect
func (c *Config)initConnect(){

}

// watch机制
func Watch(){
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := cli.Put(ctx, "sample_key", "sample_value")
	cancel()
	if err != nil {
		// handle error!
	}
}



cli, err := clientv3.New(clientv3.Config{
	Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
	DialTimeout: 5 * time.Second,
})
if err != nil {
	// handle error!
}
defer cli.Close()


ctx, cancel := context.WithTimeout(context.Background(), timeout)
resp, err := cli.Put(ctx, "sample_key", "sample_value")
cancel()
if err != nil {
    // handle error!
}
// use the response
