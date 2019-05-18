package server

import (
	"fmt"
	"io"
	"net"

	"github.com/chrisxrepo/rlink/proto"

	"github.com/chrisxrepo/goutils/pool"
)

type RedisServer struct {
	Addr string
	Port int
}

func (s *RedisServer) StartServer() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Addr, s.Port))
	if err != nil {
		panic(err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		task := &pool.Task{
			Handle: doTcpConn,
			Arg:    conn,
		}
		pool.DefaultRoutinePool.DoTask(task)
	}
}

func doTcpConn(arg interface{}) {
	conn := arg.(net.Conn)
	if conn == nil {
		return
	}

	buf := pool.DefaultBufferPool.Get()
	defer pool.DefaultBufferPool.Put(buf)
	defer conn.Close()

	redisCommand := proto.NewRedisCommand()
	for {
		n, e := buf.ReadFrom(conn)
		if e != nil && e == io.EOF {
			fmt.Println("finish 1")
			break
		}
		if n == 0 && e == nil {
			fmt.Println("finish 2")
			break
		}

		r, e := redisCommand.ParseCommand(buf)
		if e != nil {
			proto.ReplyError(conn, e)
			return
		}

		if r == false {
			continue
		}

		cmd := redisCommand.Cmd()
		//quit
		if cmd == nil || (len(cmd) == 4 && cmd[0] == 'q' && cmd[1] == 'u' && cmd[2] == 'i' && cmd[3] == 't') {
			break
		}

		redisCommand.Reset()
	}
}
