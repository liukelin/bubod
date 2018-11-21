package main
import (
	"bubod/Bubod/mq/disque"
	"log"
)

func main(){
	// mqConf := &disque.MqConf{
	// 	Servers: []string{"127.0.0.1:7712"},
	// 	Qname: "test1"}
	servers := []string{"127.0.0.1:7712"}
	qname := "test1"
	mq := disque.Connect(servers, qname)
	err := mq.Push("test")
	if err != nil {
		log.Printf("========", err)
	}


}

