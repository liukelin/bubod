# bubod ---- MySQL/MariaDB binlog时间解析 


* 支持同步到多种第三方服务, for example:

* Redis
* RabbitMQ
* Disque
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
`````

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

##### 参考资料
`````
 documentation: https://dev.mysql.com/doc/internals/en/replication-protocol.html
 binlog事件类型： https://dev.mysql.com/doc/internals/en/binlog-event.html
 https://dev.mysql.com/doc/internals/en/row-based-replication.html

 go-mysql提供的相关MySQL协议解析库。
 https://github.com/siddontang/go-mysql

 python实现
 https://github.com/noplay/python-mysql-replication

`````

##### 其他建议
`````
    1.单节点的顺序解析消费尽可能的保证了数据变化的有序
    2.作为队列消费端，对变更事件的多进程消费的话可能导致消费乱序
    3.最终建议，将此控件只是作为一个数据变更事件通知中间件，而不是数据同步工具
    3.收到该工具数据变更时间后，消费端应该使用数据id再去DB查询当前数据，才能有效的保证数据一致
`````


