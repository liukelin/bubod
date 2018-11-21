package main
import (
	// "bubod/Bubod/lib"
	// "fmt"
	"log"
	// "strings"
	"os"
	"io"
)

func main() {
	// s := "127.0.0.1:8080,127.0.0.1:8080"
	// f := strings.Split(s, ",")
	// fmt.Printf("====",f)

	fileNamePos := "2345678"
	f, err := os.OpenFile("/Users/liukelin/Desktop/go/gocode/src/bubod/data/test.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777) //打开文件
	if err !=nil {
		log.Println("[error] Write bubod_dump_pos OpenFile error; Error:",err)
	}else{
		_, err := io.WriteString(f, fileNamePos)
		if err !=nil {
			log.Println("[error] Write bubod_dump_pos WriteString err:",err)
		}
		// f.Close()
	}	
	

}