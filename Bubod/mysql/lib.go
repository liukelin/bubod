package mysql

import (
	"database/sql/driver"
	"fmt"
)
// 表字段描述
type column_schema_type struct {
	COLUMN_NAME        string	// 字段名
	COLLATION_NAME     string	// 排序规则，如：utf8_general_ci
	CHARACTER_SET_NAME string	// 编码 如：utf8
	COLUMN_COMMENT     string
	COLUMN_KEY         string	// 约束类型，PRI主键约束、UNI唯一约束、MUL可以重复、没有主键则为空 "“（没有用户手动设置的主键，mysql自身优化创建的主键无法获取）
	COLUMN_TYPE        string	// 字段类型 如：int(10)、varchar(16)、int(11) unsigned、float(9,2)
	NUMERIC_SCALE      string   // 浮点数精确多少数
	enum_values        []string
	set_values         []string
	is_bool            bool
	is_primary         bool		// 是否索引，COLUMN_KEY非空时候此值为true
	unsigned 		   bool
	auto_increment     bool		// 是否自增列
}

type MysqlConnection interface {
	DumpBinlog(filename string, position uint32, parser *eventParser, callbackFun callback, result chan error) (driver.Rows, error)
	Close() error
	Ping() error
	Prepare(query string) (driver.Stmt, error)
	Exec(query string,args []driver.Value)  (driver.Result, error)
}
// 事件内容
type EventReslut struct {
	Header         EventHeader
	Rows           []map[string]driver.Value // 变更数据
	Query          string	// sql
	SchemaName     string	// 库
	TableName      string	// 表
	BinlogFileName string
	BinlogPosition uint32
	Primary		   string	// 主键字段
	// ColumnSchemaType	  *column_schema_type // 表字段属性
}
// 事件回调
type callback func(data *EventReslut)


func fieldTypeName(t FieldType) string {
	switch t {
	case FIELD_TYPE_DECIMAL:
		return "FIELD_TYPE_DECIMAL"
	case FIELD_TYPE_TINY:
		return "FIELD_TYPE_TINY"
	case FIELD_TYPE_SHORT:
		return "FIELD_TYPE_SHORT"
	case FIELD_TYPE_LONG:
		return "FIELD_TYPE_LONG"
	case FIELD_TYPE_FLOAT:
		return "FIELD_TYPE_FLOAT"
	case FIELD_TYPE_DOUBLE:
		return "FIELD_TYPE_DOUBLE"
	case FIELD_TYPE_NULL:
		return "FIELD_TYPE_NULL"
	case FIELD_TYPE_TIMESTAMP:
		return "FIELD_TYPE_TIMESTAMP"
	case FIELD_TYPE_LONGLONG:
		return "FIELD_TYPE_LONGLONG"
	case FIELD_TYPE_INT24:
		return "FIELD_TYPE_INT24"
	case FIELD_TYPE_DATE:
		return "FIELD_TYPE_DATE"
	case FIELD_TYPE_TIME:
		return "FIELD_TYPE_TIME"
	case FIELD_TYPE_DATETIME:
		return "FIELD_TYPE_DATETIME"
	case FIELD_TYPE_YEAR:
		return "FIELD_TYPE_YEAR"
	case FIELD_TYPE_NEWDATE:
		return "FIELD_TYPE_NEWDATE"
	case FIELD_TYPE_VARCHAR:
		return "FIELD_TYPE_VARCHAR"
	case FIELD_TYPE_BIT:
		return "FIELD_TYPE_BIT"
	case FIELD_TYPE_NEWDECIMAL:
		return "FIELD_TYPE_NEWDECIMAL"
	case FIELD_TYPE_ENUM:
		return "FIELD_TYPE_ENUM"
	case FIELD_TYPE_SET:
		return "FIELD_TYPE_SET"
	case FIELD_TYPE_TINY_BLOB:
		return "FIELD_TYPE_TINY_BLOB"
	case FIELD_TYPE_MEDIUM_BLOB:
		return "FIELD_TYPE_MEDIUM_BLOB"
	case FIELD_TYPE_LONG_BLOB:
		return "FIELD_TYPE_LONG_BLOB"
	case FIELD_TYPE_BLOB:
		return "FIELD_TYPE_BLOB"
	case FIELD_TYPE_VAR_STRING:
		return "FIELD_TYPE_VAR_STRING"
	case FIELD_TYPE_STRING:
		return "FIELD_TYPE_STRING"
	case FIELD_TYPE_GEOMETRY:
		return "FIELD_TYPE_GEOMETRY"
	}
	return fmt.Sprintf("%d", t)
}