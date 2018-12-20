# bubod ---- MySQL/Maria 数据变更增量事件获取 


* 支持同步到多种第三方服务, for example:

* Redis
* RabbitMQ
* Kafka
* Http 


### MySQL 版本

* 5.1.73

* 5.5.42

* 5.6.36

* 5.7.18

* 8.0.11


### 安装

##### 编译

```sh

go get github.com/liukelin/Bubod

# windows
./build.sh windows

# linux
./build.sh linux

#mac
./build.sh darwin

#freebsd
./build.sh freebsd

```

##### 配置
`````sh

vim Bubod.ini

[Bubod]
; 集群名称
cluster_name=bubod

debug=true

daemon=false

# 默认会当前启动文件夹./logs
log_dir=/Users/liukelin/Desktop/go/gocode/src/bubod/logs

# 默认会当前启动文件夹
data_dir=/Users/liukelin/Desktop/go/gocode/src/bubod/data

# 默认启动文件夹下 bubod.pid
# pid=/tmp/bubod-{cluster_name}-{server_id}.pid

[Database]
# 实例唯一id(相当于mysql server_id，也用于作为node-name)
server_id=2
host=127.0.0.1
port=3306
user=root
pass=
db=bubod_test

# 需要订阅的tables
tables=
# 排除的tables
filter_tables=

# 开始同步的位点 mysql-bin.000003  120
binlog_dump_file_name=mysql-bin.000003
binlog_dump_position=120

[Channel]
# 队列名称,默认为cluster_name
qname=bubod
# 使用队列类型 disque/rabbit
type=disque
# 127.0.0.1:8010,127.0.0.1:8011
servers=127.0.0.1:7711
# type=rabbit
# amqp=amqp://guest:guest@localhost:5672/


# 高可用下 Zookeeper 配置
[Zookeeper]
# 服务地址 127.0.0.1:2181,127.0.0.1:2182 
server=127.0.0.1:2181


`````

##### 启动
`````shell
./Bubod-server -config=bubod.ini

`````

##### 停止

`````


