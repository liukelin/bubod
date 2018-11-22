#/bin/bash
cd `dirname $0`
mode=$1
case "$mode" in
    'windows')
        ;;
    'linux')
        ;;
    'darwin')
        ;;
    'freebsd')
        ;;
    *)
        $mode = "linux"
        ;;
esac

mkdir -p $1/etc


CGO_ENABLED=0 GOOS=$mode GOARCH=amd64 go build ./main.go

if [[ "$1" == "windows" ]];then
    mv Bubod.exe ./windows
    cp -f ./etc/bubod.ini ./$1/etc
else
    mv ./Bubod ./$1
    cp -f ./etc/bubod.ini ./$1/etc
    cp -f ./Bubod-server ./$1
fi

echo "$1 build over"

