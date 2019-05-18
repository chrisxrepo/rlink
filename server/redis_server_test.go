package server

import "testing"

func TestRedisServer(t *testing.T) {
	rs := &RedisServer{
		Addr: "127.0.0.1",
		Port: 8888,
	}

	rs.StartServer()
}
