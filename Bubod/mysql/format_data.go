package mysql
import (
	"fmt"
	// "log"
	"database/sql/driver"
	"encoding/json"
)

/*
最终输出格式
{"binlog":"mysql-bin.000004:786","db":"test","table":"test1","query":"","event_type":"update","before":{"id":2,"num":1,"strs":"wddd","times":"-0001-11-30 00:00:00"},"after":{"id":2,"num":1,"strs":"wddd","times":"2018-09-14 00:00:00"}}

*/
type FormatDataJsonStruct struct {
	Binlog		string 	`json:"binlog"`
	Db  		string 	`json:"db"`
	Table  	 	string  `json:"table"`		// table
	Query		string	`json:"query"`		// 如果非 insert、update、delete.则返回操作sql
	EventType  	string 	`json:"event_type"`	// 操作类型 insert、update、delete、空
	Primary		string	`json:"primary"`	// 主键字段；EventType非空时有值
	Before 		map[string]driver.Value `json:"before"`	// 变更前数据
	After		map[string]driver.Value `json:"after"`	// 变更后数据
	Timestamp	uint32	`json:"timestamp"`	// 事件事件
}

// 自定义类型name
func EvenTypeName(e EventType) string {
	switch e {
	case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
		return "insert"
	case UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		return "update"
	case DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		return "delete"
	}
	return fmt.Sprintf("%d", e)
}

// 转换json
func FormatEventDataJson(data *FormatDataJsonStruct) string {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Println("encoding faild")
	} else {
		return string(b)
	}
	return ""
}

// 拆分组装数据
func FormatEventData(data *EventReslut) []string {
	if len(data.Rows)<1 {
		return nil
	}

	binlog := fmt.Sprintf("%s:%d", data.BinlogFileName, data.BinlogPosition)
	eventType := EvenTypeName(data.Header.EventType)
	
	formatDataJsonStruct := &FormatDataJsonStruct{
		Binlog:		binlog,
		Db:  		data.SchemaName,
		Table:  	data.TableName,
		EventType: 	eventType,
		Query:		"",
		Primary:	data.Primary,
		Before:		make(map[string]driver.Value),
		After:		make(map[string]driver.Value),
		Timestamp:	data.Header.Timestamp,
	}
	var formatEventDatas = make([]string, 0)
	switch eventType {
	case "insert", "delete":

		// var formatEventDatas = make([]string, len(data.Rows))
		for _, row := range data.Rows {
			_data := formatDataJsonStruct
			_data.Before = row
			formatEventDatas = append(formatEventDatas, FormatEventDataJson(_data))
		}

	case "update":
		
		// var formatEventDatas = make([]string, len(data.Rows)/2)
		for k, row := range data.Rows {
			if k%2 == 1 { // 奇数
				_data := formatDataJsonStruct
				_data.Before = row			// data.Rows[k-1]
				_data.After = data.Rows[k]
				formatEventDatas = append(formatEventDatas, FormatEventDataJson(_data))
			}
		}
	default: // 其他事件类型只返回sql语句
		_data := formatDataJsonStruct
		_data.Query = data.Query
		formatEventDatas = append(formatEventDatas, FormatEventDataJson(_data))
		
	}

	return formatEventDatas
}

/*
{
	"binlog": "mysql-bin.000005:4",
	"db": "bifrost_test",
	"table": "test3",
	"query": "",
	"event_type": "update",
	"is_primary":, // 主键字段
	"before": {
		"id": 1,
		"test_unsinged_bigint": 5,
		"test_unsinged_int": 4,
		"test_unsinged_mediumint": 3,
		"test_unsinged_smallint": 2,
		"test_unsinged_tinyint": 1,
		"testbigint": -5,
		"testbit": 8,
		"testblob": "testblob",
		"testbool": true,
		"testchar": "te",
		"testdate": "2018-05-08",
		"testdatetime": "2018-05-08 15:30:21",
		"testdecimal": "9.39",
		"testdouble": 9.39,
		"testenum": "en2",
		"testfloat": 9.39,
		"testint": -4,
		"testlongblob": "testlongblob",
		"testmediumblob": "testmediumblob",
		"testmediumint": -3,
		"testset": ["set1", "set3"],
		"testsmallint": -2,
		"testtext": "testtext",
		"testtime": "15:39:59",
		"testtimestamp": "2018-05-08 15:30:21",
		"testtinyblob": "testtinyblob",
		"testtinyint": -1,
		"testvarchar": "testvarcha",
		"testyear": "2018"
	},
	"after": {
		"id": 1,
		"test_unsinged_bigint": 5,
		"test_unsinged_int": 4,
		"test_unsinged_mediumint": 3,
		"test_unsinged_smallint": 2,
		"test_unsinged_tinyint": 1,
		"testbigint": -5,
		"testbit": 8,
		"testblob": "testblob",
		"testbool": true,
		"testchar": "te",
		"testdate": "2018-05-08",
		"testdatetime": "2018-05-08 15:30:21",
		"testdecimal": "9.39",
		"testdouble": 9.39,
		"testenum": "en2",
		"testfloat": 9.39,
		"testint": -4,
		"testlongblob": "testlongblob",
		"testmediumblob": "testmediumblob",
		"testmediumint": -5,
		"testset": ["set1", "set3"],
		"testsmallint": -2,
		"testtext": "testtext",
		"testtime": "15:39:59",
		"testtimestamp": "2018-05-08 15:30:21",
		"testtinyblob": "testtinyblob",
		"testtinyint": -1,
		"testvarchar": "testvarcha",
		"testyear": "2018"
	}
}
*/