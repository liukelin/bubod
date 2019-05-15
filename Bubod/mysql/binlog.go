// documentation:
// https://dev.mysql.com/doc/internals/en/replication-protocol.html
// binlog时间类型 https://dev.mysql.com/doc/internals/en/binlog-event.html
// https://dev.mysql.com/doc/internals/en/row-based-replication.html
package mysql

import (
	"bytes"
	"database/sql/driver"
	"log"
	"strings"
	"encoding/hex"
	"fmt"
	"time"
	"sync"
	"strconv"
)

type eventParser struct {
	format           	*FormatDescriptionEvent				// 事件描述
	tableMap         	map[uint64]*TableMapEvent			// table字段详情
	tableNameMap     	map[string]uint64					// table字段集合
	tableSchemaMap   	map[uint64][]*column_schema_type	// table字段描述详情
	dataSource       	*string
	connStatus       	int8 //连接状态 0 stop  1 running
	conn             	MysqlConnection
	dumpBinLogStatus 	uint8 // 同步状态 0 stop , 1 running, 2 mysqlConn.Close, 3 KillConnect mysqlConn.Close
	binlogFileName   	string
	binlogPosition   	uint32
	maxBinlogFileName   string
	maxBinlogPosition   uint32
	binlogIgnoreDb   	*string
	replicateDoDb    	map[string]uint8
	eventDo          	[]bool		// 订阅的事件
	ServerId        	uint32
	connectionId	 	string
	connLock 		 	sync.Mutex
	binlog_checksum  	bool
}

func newEventParser() (parser *eventParser) {
	parser = new(eventParser)
	parser.tableMap = make(map[uint64]*TableMapEvent)
	parser.tableNameMap = make(map[string]uint64)
	parser.tableSchemaMap = make(map[uint64][]*column_schema_type)
	parser.eventDo = make([]bool, 36, 36)
	parser.ServerId = 1
	parser.connectionId = ""
	parser.maxBinlogFileName = ""
	parser.maxBinlogPosition = 0
	parser.binlog_checksum = false
	return
}

// binlog事件内容解析
func (parser *eventParser) parseEvent(data []byte) (event *EventReslut, filename string, err error) {
	var buf *bytes.Buffer
	if parser.binlog_checksum {
		buf = bytes.NewBuffer(data[0:len(data)-4])
	}else{
		buf = bytes.NewBuffer(data)
	}

	filename = parser.binlogFileName
	switch EventType(data[4]) {
	case HEARTBEAT_EVENT,IGNORABLE_EVENT,GTID_EVENT,ANONYMOUS_GTID_EVENT,PREVIOUS_GTIDS_EVENT:
		// 其余主 主动更新事件
		return
	case FORMAT_DESCRIPTION_EVENT:
		// 格式描述事件
		parser.format, err = parser.parseFormatDescriptionEvent(buf)
		/*
		i := strings.IndexAny(parser.format.mysqlServerVersion, "-")
		var version string
		if i> 0{
			version = parser.format.mysqlServerVersion[0:i]
		}else{
			version = parser.format.mysqlServerVersion
		}
		if len(version)==5{
			version = strings.Replace(version, ".", "", 1)
			version = strings.Replace(version, ".", "0", 1)
		}else{
			version = strings.Replace(version, ".", "", -1)
		}
		parser.mysqlVersionInt,err = strconv.Atoi(version)
		if err != nil{
			log.Println("mysql version:",version,"err",err)
		}
		*/
		//log.Println("binlogVersion:",parser.format.binlogVersion,"server version:",parser.format.mysqlServerVersion)
		event = &EventReslut{
			Header: parser.format.header,
		}
		return
	case QUERY_EVENT:
		// 其他变更结构sql 
		var queryEvent *QueryEvent
		queryEvent, err = parser.parseQueryEvent(buf)
		event = &EventReslut{
			Header:         queryEvent.header,
			SchemaName:     queryEvent.schema,
			BinlogFileName: parser.binlogFileName,
			TableName:      "",
			Query:          queryEvent.query,
		}
		return
	case ROTATE_EVENT: 
		// 切换新binlogFileName
		var rotateEvent *RotateEvent
		rotateEvent, err = parser.parseRotateEvent(buf)
		parser.binlogFileName = rotateEvent.filename
		parser.binlogPosition = uint32(rotateEvent.position)
		filename = parser.binlogFileName
		event = &EventReslut{
			Header:         rotateEvent.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
		}
		// 清空 表字段map，避免字段串表（不同binlog文件可能Tableid 对应关系不同）
		parser.tableSchemaMap = make(map[uint64][]*column_schema_type,0)
		return
	case TABLE_MAP_EVENT:
		// 表变更事件，填充表、表字段 的详细信息，每个rows 事件均会带上此事件
		// https://dev.mysql.com/doc/internals/en/table-map-event.html
		var table_map_event *TableMapEvent
		table_map_event, err = parser.parseTableMapEvent(buf)
		parser.tableMap[table_map_event.tableId] = table_map_event
		if _, ok := parser.tableSchemaMap[table_map_event.tableId]; !ok {
			parser.GetTableSchema(table_map_event.tableId, table_map_event.schemaName, table_map_event.tableName)
		}
		event = &EventReslut{
			Header:         table_map_event.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
			SchemaName:     parser.tableMap[table_map_event.tableId].schemaName,
			TableName:      parser.tableMap[table_map_event.tableId].tableName,
		}
		return
	case WRITE_ROWS_EVENTv0,WRITE_ROWS_EVENTv1,WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv0,UPDATE_ROWS_EVENTv1,UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv0,DELETE_ROWS_EVENTv1,DELETE_ROWS_EVENTv2:
		
		// insert update delete 数据变更事件处理
		var rowsEvent *RowsEvent
		rowsEvent, err = parser.parseRowsEvent(buf)
		if err != nil{
			log.Println("row event err:",err)
		}
		// log.Println("############:",parser.tableMap[rowsEvent.tableId].tableName)
		// log.Println("############:",rowsEvent.tableId)
		// log.Println("############:",buf)
		
		event = &EventReslut{
			Header:         rowsEvent.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
			SchemaName:     parser.tableMap[rowsEvent.tableId].schemaName,
			TableName:      parser.tableMap[rowsEvent.tableId].tableName,
			Rows:           rowsEvent.rows,
			Primary:        rowsEvent.primary,
		}
	default:
		var genericEvent *GenericEvent
		genericEvent, err = parseGenericEvent(buf)
		event = &EventReslut{
			Header: genericEvent.header,
		}
	}
	return
}

func (parser *eventParser) initConn() {
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(*parser.dataSource)
	if err != nil {
		panic(err)
	} else {
		parser.connStatus = 1
	}
	parser.conn = conn.(MysqlConnection)
}

func (parser *eventParser) GetTableSchema(tableId uint64, database string, tablename string) {
	for {
		parser.connLock.Lock()
		err := parser.GetTableSchemaByName(tableId,database,tablename)
		parser.connLock.Unlock()
		if err == nil{
			break
		}
	}
}

// 根据 tablename 获取完整表字段/描述
func (parser *eventParser) GetTableSchemaByName(tableId uint64, database string, tablename string) (errs error) {
	errs = fmt.Errorf("unknow error")
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			errs = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	//set dbAndTable Name tableId
	parser.tableNameMap[database+"."+tablename] = tableId
	sql := "SELECT COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA FROM information_schema.columns WHERE table_schema='" + database + "' AND table_name='" + tablename + "' ORDER BY `ORDINAL_POSITION` ASC"
	stmt, err := parser.conn.Prepare(sql)
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		errs = err
		return
	}
	for {
		dest := make([]driver.Value, 7, 7)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME, COLUMN_KEY, COLUMN_TYPE string
		var CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA string
		var isBool bool = false
		var unsigned bool = false
		var is_primary bool = false
		var auto_increment bool = false
		var enum_values, set_values []string

		COLUMN_NAME = string(dest[0].([]byte))
		COLUMN_KEY = string(dest[1].([]byte))
		COLUMN_TYPE = string(dest[2].([]byte))
		CHARACTER_SET_NAME = string(dest[3].([]byte))
		COLLATION_NAME = string(dest[4].([]byte))
		NUMERIC_SCALE = string(dest[5].([]byte))
		EXTRA = string(dest[6].([]byte))
		if COLUMN_TYPE == "tinyint(1)"{
			isBool = true
		}
		if EXTRA == "auto_increment"{
			auto_increment = true
		}
		if strings.Contains(COLUMN_TYPE,"unsigned"){
			unsigned = true
		}
		if COLUMN_KEY != ""{
			is_primary = true
		}

		if COLUMN_TYPE[0:4] == "enum" {
			d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			enum_values = strings.Split(d, ",")
		} else {
			enum_values = make([]string, 0)
		}

		if COLUMN_TYPE[0:3] == "set" {
			d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			set_values = strings.Split(d, ",")
		} else {
			set_values = make([]string, 0)
		}
		parser.tableSchemaMap[tableId] = append(parser.tableSchemaMap[tableId], &column_schema_type{
			COLUMN_NAME: COLUMN_NAME,
			COLUMN_KEY:  COLUMN_KEY,
			COLUMN_TYPE: COLUMN_TYPE,
			enum_values: enum_values,
			set_values:  set_values,
			is_bool:	 isBool,
			unsigned:    unsigned,
			is_primary:  is_primary,
			auto_increment: auto_increment,
			CHARACTER_SET_NAME:CHARACTER_SET_NAME,
			COLLATION_NAME:COLLATION_NAME,
			NUMERIC_SCALE:NUMERIC_SCALE,
		})
	}
	rows.Close()
	errs = nil
	return
}


/*
获取当前连接的binlog同步状态
+------+------------------------------------------------------------------+
| TIME | STATE                                                            |
+------+------------------------------------------------------------------+
| 1049 | Master has sent all binlog to slave; waiting for binlog to be up |
+------+------------------------------------------------------------------+
*/
func (parser *eventParser) GetConnectionInfo(connectionId string) (m map[string]string){
	parser.connLock.Lock()
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			parser.connLock.Unlock()
			log.Println("binlog.go GetConnectionInfo err:",err)
			m = nil
		}else{
			parser.connLock.Unlock()
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	sql := "select TIME,STATE from `information_schema`.`PROCESSLIST` WHERE ID='"+connectionId+"'"
	stmt, err := parser.conn.Prepare(sql)
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		return nil
	}
	m = make(map[string]string,2)
	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		m["TIME"] = string(dest[0].([]byte))
		m["STATE"]=string(dest[1].([]byte))
		break
	}
	return
}


func (parser *eventParser) KillConnect(connectionId string) (b bool){
	b = false
	parser.connLock.Lock()
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			parser.connLock.Unlock()
			b = false
		}else{
			parser.connLock.Unlock()
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	sql := "kill "+connectionId
	p := make([]driver.Value, 0)
	_, err := parser.conn.Exec(sql,p)
	if err != nil {
		return false
	}
	return true
}

func (parser *eventParser) GetTableId(database string, tablename string) uint64 {
	key := database + "." + tablename
	if _, ok := parser.tableNameMap[key]; !ok {
		return uint64(0)
	}
	return parser.tableNameMap[key]
}

func (parser *eventParser) GetQueryTableName(sql string) (string, string) {
	sql = strings.Trim(sql, " ")
	if len(sql) < 11 {
		return "", ""
	}
	if strings.ToUpper(sql[0:11]) == "ALTER TABLE" {
		sqlArr := strings.Split(sql, " ")
		dbAndTable := strings.Replace(sqlArr[2], "`", "", -1)
		i := strings.IndexAny(dbAndTable, ".")
		var databaseName, tablename string
		if i > 0 {
			databaseName = dbAndTable[0:i]
			tablename = dbAndTable[i+1:]
		} else {
			databaseName = ""
			tablename = dbAndTable
		}
		return databaseName, tablename
	}
	return "", ""
}

// 开始同步
func (mc *mysqlConn) DumpBinlog(filename string, position uint32, parser *eventParser, callbackFun callback, result chan error) (driver.Rows, error) {
	/*
	defer func() {
		if err := recover(); err != nil {
			log.Println("DumpBinlog err:",err,313)
			result <- fmt.Errorf(fmt.Sprint(err))
			return
		}
	}()
	*/

	// log.Println("start DumpBinlog...")
	ServerId := uint32(parser.ServerId) // Must be non-zero to avoid getting EOF packet
	flags := uint16(0)
	e := mc.writeCommandPacket(COM_BINLOG_DUMP, position, flags, ServerId, filename)
	if e != nil {
		result <- e
		return nil, e
	}

	// 持续等待获取事件
	for {
		if parser.dumpBinLogStatus != 1 {
			if parser.dumpBinLogStatus == 0 {
				time.Sleep(1 * time.Second)
				result <- fmt.Errorf("stop")
				continue
			}
			if parser.dumpBinLogStatus == 2 {
				result <- fmt.Errorf("close")
				break
			}
		}
		
		pkt, e := mc.readPacket()
		if e != nil {
			result <- e
			return nil, e
		} else if pkt[0] == 254 { // EOF packet
			result <- fmt.Errorf("EOF packet")
			break
		}
		if pkt[0] == 0 {
			event, _, e := parser.parseEvent(pkt[1:])
			if e != nil {
				fmt.Println("parseEvent err:",e)
				result <- e
				return nil, e
			}
			if event == nil{
				continue
			}

			//QUERY_EVENT ,must be read Schema again
			if event.Header.EventType == QUERY_EVENT {
				if SchemaName, tableName := parser.GetQueryTableName(event.Query); tableName != "" {
					if SchemaName != "" {
						event.SchemaName = SchemaName
					}
					event.TableName = tableName
					if tableId := parser.GetTableId(event.SchemaName, tableName); tableId > 0 {
						parser.GetTableSchema(tableId, event.SchemaName, tableName)
					}
				}
			}
			
			//only return replicateDoDb, any sql may be use db.table query
			if len(parser.replicateDoDb) > 0 {
				if _, ok := parser.replicateDoDb[event.SchemaName]; !ok {
					continue
				}
			}

			//only return EventType by set
			if parser.eventDo[int(event.Header.EventType)] == false {
				continue
			}

			if event.BinlogFileName == parser.maxBinlogFileName && event.Header.LogPos >= parser.maxBinlogPosition{
				parser.dumpBinLogStatus = 2
				break
			}
			//set binlog info
			// log.Println("=========xxxx: ",event.Header.EventType)
			callbackFun(event)
			parser.binlogFileName = event.BinlogFileName
			parser.binlogPosition = event.Header.LogPos

		} else {
			result <- fmt.Errorf("Unknown packet:\n%s\n\n", hex.Dump(pkt))
			if strings.Contains(string(pkt),"Could not find first log file name in binary log index file"){
				result <- fmt.Errorf("close")
				break
			}
			//result <- fmt.Errorf("Unknown packet:\n%s\n\n", hex.Dump(pkt))
		}
	}
	return nil, nil
}

type BinlogDump struct {
	DataSource 		string
	Status     		string //stop,running,close,error,starting
	parser     		*eventParser
	//BinlogIgnoreDb string
	ReplicateDoDb 	map[string]uint8
	OnlyEvent     	[]EventType	// 订阅事件类型
	CallbackFun   	callback		// 回调函数
	mysqlConn  		MysqlConnection
	mysqlConnStatus int
	connLock 		sync.Mutex
}

func (This *BinlogDump) StartDumpBinlog(filename string, position uint32, ServerId uint32, result chan error,maxFileName string,maxPosition uint32) {
	
	This.parser = newEventParser()
	This.parser.dataSource = &This.DataSource
	This.parser.connStatus = 0
	This.parser.dumpBinLogStatus = 1
	This.parser.replicateDoDb = This.ReplicateDoDb
	This.parser.ServerId = ServerId
	This.parser.maxBinlogPosition = maxPosition
	This.parser.maxBinlogFileName = maxFileName
	for _, val := range This.OnlyEvent {
		This.parser.eventDo[int(val)] = true
	}

	defer func() {
		This.parser.connLock.Lock()
		if This.parser.connStatus == 1 {
			This.parser.connStatus = 0
			This.parser.conn.Close()
		}
		This.parser.connLock.Unlock()
	}()
	This.parser.binlogFileName = filename
	This.parser.binlogPosition = position
	for {
		if This.parser.dumpBinLogStatus == 3 {
			break
		}
		if This.parser.dumpBinLogStatus == 2 {
			result <- fmt.Errorf("close")
			break
		}

		result <- fmt.Errorf("starting")
		This.startConnAndDumpBinlog(result)
		time.Sleep(2 * time.Second)
	}
}

/*
replication event checksum
binlog验证 设置
mysql5.6.5以后的版本中binlog_checksum默认值是crc32
而之前的版本binlog_checksum默认值是none
mysql> SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM';
+-----------------+-------+
| Variable_name   | Value |
+-----------------+-------+
| binlog_checksum | CRC32 |
+-----------------+-------+
*/
func (This *BinlogDump) checksum_enabled() {
	sql := "SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"
	stmt, err := This.mysqlConn.Prepare(sql)
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Println("checksum_enabled sql query err:",err)
		return
	}
	dest := make([]driver.Value, 2, 2)
	err = rows.Next(dest)
	if err != nil {
		if err.Error() != "EOF"{
			log.Println("checksum_enabled err:",err)
		}
		return
	}
	
	if string(dest[1].([]byte)) != ""{
		This.mysqlConn.Exec("set @master_binlog_checksum= @@global.binlog_checksum",p)
		This.parser.binlog_checksum = true
	}

	return
}

/*
// 获取mysql master 最后位点信息
// [file, position]
mysql> SHOW MASTER STATUS;
+------------------+----------+--------------+------------------+-------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
+------------------+----------+--------------+------------------+-------------------+
| mysql-bin.000021 |     1573 |              |                  |                   |
+------------------+----------+--------------+------------------+-------------------+
*/
func (This *BinlogDump) getMasterFilePosition() []string {
	sql := "SHOW MASTER STATUS;"
	stmt, err := This.mysqlConn.Prepare(sql)
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Println("[error] show master status, sql query error:",err)
		return nil
	}
	dest := make([]driver.Value, 4, 4)
	err = rows.Next(dest)
	if err != nil {
		if err.Error() != "EOF"{
			log.Println("getMasterFilePosition err:", err)
		}
		return nil
	}
	if string(dest[0].([]byte)) != "" && string(dest[1].([]byte)) != "" {
		filepos := []string { string(dest[0].([]byte)), string(dest[1].([]byte)) } 
		return filepos
	}
	return nil
}

/*
 获取当前连接id
mysql> SELECT connection_id();
+-----------------+
| connection_id() |
+-----------------+
|              87 |
+-----------------+
 */
func (This *BinlogDump) startConnAndDumpBinlog(result chan error) {
	
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(This.DataSource)
	if err != nil {
		result <- err
		time.Sleep(5 * time.Second)

		log.Println("mysqlConn err:", err)
		return
	}
	This.mysqlConn = conn.(MysqlConnection)

	//*** get connection id start
	sql := "SELECT connection_id()"
	stmt, err := This.mysqlConn.Prepare(sql)
	if err != nil{
		result <- err
		log.Println("[error] SELECT connection_id() err:", err)
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	var connectionId string
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			log.Println("[error] row Next err:", err)
			break
		}
		connectionId = string(dest[0].([]byte))
		break
	}
	log.Println("connectionId:", connectionId)
	if connectionId == ""{
		log.Println("[error] connectionId:null")
		return
	}

	result <- fmt.Errorf("running") // 消息需要及时消费，否则是阻塞
	This.parser.connectionId = connectionId
	//go This.checkDumpConnection(connectionId) //检查同步状态
	//*** get connection id end

	// 如果传入的pos为空，则使用当前最新
	if This.parser.binlogFileName==""{
		filepos := This.getMasterFilePosition()
		if len(filepos) >=2 {
			pos, err := strconv.ParseUint(filepos[1], 10, 64)
			if err != nil {
				log.Println("[error] getMasterFilePosition ParseUint pos error:", err)
			}else{
				This.parser.binlogFileName = filepos[0]
				This.parser.binlogPosition = uint32(pos)
			}
		}
	}

	This.checksum_enabled()
	This.mysqlConn.DumpBinlog(This.parser.binlogFileName, This.parser.binlogPosition, This.parser, This.CallbackFun, result)
	This.connLock.Lock()
	if This.mysqlConn != nil {
		This.mysqlConn.Close()
		This.mysqlConn = nil
	}
	This.connLock.Unlock()
	switch This.parser.dumpBinLogStatus {
	case 3:
		break
	case 2:
		result <- fmt.Errorf("close")
		This.Status = "close"
		break
	default:
		result <- fmt.Errorf("starting")
		This.Status = "stop"
	}
	This.parser.KillConnect(This.parser.connectionId)
}

/**
 检查同步状态
*/
func (This *BinlogDump) checkDumpConnection(connectionId string) {
	defer func() {
		if err := recover();err !=nil{
			log.Println("binlog.go checkDumpConnection err:",err)
		}
	}()
	for{
		time.Sleep(9 * time.Second)
		if This.parser.dumpBinLogStatus >= 2{
			break
		}
		var m map[string]string
		for i:=0;i<3;i++{
			m = This.parser.GetConnectionInfo(connectionId)
			if m == nil{
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
		//log.Println("GetConnectionInfo:",m)
		This.parser.connLock.Lock()
		if connectionId != This.parser.connectionId{
			This.parser.connLock.Unlock()
			break
		}
		if m == nil || m["TIME"] == ""{
			log.Println("This.mysqlConn close ,connectionId: ",connectionId)
			This.connLock.Lock()
			if This.mysqlConn != nil{
				This.mysqlConn.Close()
				This.mysqlConn = nil
			}
			This.connLock.Unlock()
			break
		}
		This.parser.connLock.Unlock()
	}
}


func (This *BinlogDump) Stop() {
	This.parser.dumpBinLogStatus = 0
}

func (This *BinlogDump) Start() {
	This.parser.dumpBinLogStatus = 1
}

func (This *BinlogDump) Close() {
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	This.connLock.Lock()
	defer This.connLock.Unlock()
	This.parser.dumpBinLogStatus = 2
	This.mysqlConn.Close()
	This.mysqlConn = nil
}

func (This *BinlogDump) KillDump() {
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	This.connLock.Lock()
	defer This.connLock.Unlock()
	This.parser.dumpBinLogStatus = 3
	This.parser.KillConnect(This.parser.connectionId)
	This.mysqlConn.Close()
	This.mysqlConn = nil
}