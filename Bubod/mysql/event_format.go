// https://dev.mysql.com/doc/internals/en/format-description-event.html
// A format description event is the first event of a binlog for binlog-version 4. 
// It describes how the other events are layed out.
// 事件格式描述
// 格式描述事件是.binlog-version 4 的binlog的第一个事件。它描述了其他事件是如何被渲染出来的。
package mysql

import (
	"bytes"
	"encoding/binary"
)

type FormatDescriptionEvent struct {
	header                 EventHeader
	binlogVersion          uint16
	mysqlServerVersion     string
	createTimestamp        uint32
	eventHeaderLength      uint8
	eventTypeHeaderLengths []byte
}

func (parser *eventParser) parseFormatDescriptionEvent(buf *bytes.Buffer) (event *FormatDescriptionEvent, err error) {
	event = new(FormatDescriptionEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.binlogVersion)
	event.mysqlServerVersion = string(buf.Next(50))
	err = binary.Read(buf, binary.LittleEndian, &event.createTimestamp)
	event.eventHeaderLength, err = buf.ReadByte()
	event.eventTypeHeaderLengths = buf.Bytes()
	return
}