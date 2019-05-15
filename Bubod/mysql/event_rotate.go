// https://dev.mysql.com/doc/internals/en/rotate-event.html
// binlog文件切换事件
package mysql

import (
	"bytes"
	"encoding/binary"
)

type RotateEvent struct {
	header   EventHeader
	position uint64
	filename string
}

// 返回当前的位点文件
func  (parser *eventParser) parseRotateEvent(buf *bytes.Buffer) (event *RotateEvent, err error) {
	event = new(RotateEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.position)
	event.filename = buf.String()
	return
}