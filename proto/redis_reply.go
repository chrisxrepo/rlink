package proto

import (
	"net"
	"strconv"
)

const (
	StatusReply    = '+'
	ErrorReply     = '-'
	IntegerReply   = ':'
	BulkReply      = '$'
	MultiBulkReply = '*'
)

func ReplyError(conn net.Conn, err error) error {
	errstr := err.Error()
	reply := make([]byte, 1, 3+len(errstr))
	reply[0] = ErrorReply
	reply = append(reply, errstr...)
	reply = append(reply, "\r\n"...)
	return writeReply(conn, reply)
}

func ReplyOk(conn net.Conn) error {
	reply := []byte("+OK\r\n")
	return writeReply(conn, reply)
}

func ReplyPong(conn net.Conn) error {
	reply := []byte("+PONG\r\n")
	return writeReply(conn, reply)
}

func ReplyInteger(conn net.Conn, v int) error {
	reply := make([]byte, 1, 15)
	reply[0] = IntegerReply
	reply = append(reply, strconv.FormatInt(int64(v), 10)...)
	reply = append(reply, "\r\n"...)

	return writeReply(conn, reply)
}

func ReplyBulkStr(conn net.Conn, str string) error {
	if len(str) == 0 {
		reply := []byte("$0\r\n\r\n")
		return writeReply(conn, reply)
	}

	reply := make([]byte, 1, 15+len(str))
	reply[0] = BulkReply
	reply = append(reply, strconv.FormatInt(int64(len(str)), 10)...)
	reply = append(reply, "\r\n"...)

	reply = append(reply, str...)
	reply = append(reply, "\r\n"...)
	return writeReply(conn, reply)
}

func ReplyBulkBytes(conn net.Conn, bytes []byte) error {
	if bytes == nil {
		reply := []byte("$-1\r\n")
		return writeReply(conn, reply)
	}

	if len(bytes) == 0 {
		reply := []byte("$0\r\n\r\n")
		return writeReply(conn, reply)
	}

	reply := make([]byte, 1, 15+len(bytes))
	reply[0] = BulkReply
	reply = append(reply, strconv.FormatInt(int64(len(bytes)), 10)...)
	reply = append(reply, "\r\n"...)

	reply = append(reply, bytes...)
	reply = append(reply, "\r\n"...)
	return writeReply(conn, reply)
}

func ReplyBytesArray(conn net.Conn, data [][]byte) error {
	if data == nil {
		reply := []byte("*-1\r\n")
		return writeReply(conn, reply)
	}

	if len(data) == 0 {
		reply := []byte("*0\r\n")
		return writeReply(conn, reply)
	}

	rl := 15
	for _, v := range data {
		if v == nil || len(v) == 0 {
			rl += 6
		} else {
			rl += 10
			rl += len(v)
		}
	}

	reply := make([]byte, 1, rl)
	reply[0] = MultiBulkReply
	reply = append(reply, strconv.FormatInt(int64(len(data)), 10)...)
	reply = append(reply, "\r\n"...)
	for _, v := range data {
		if v == nil {
			reply = append(reply, "$-1\r\n"...)
		} else if len(v) == 0 {
			reply = append(reply, "$0\r\n\r\n"...)
		} else {
			reply = append(reply, BulkReply)
			reply = append(reply, strconv.FormatInt(int64(len(v)), 10)...)
			reply = append(reply, "\r\n"...)

			reply = append(reply, v...)
			reply = append(reply, "\r\n"...)
		}
	}

	return writeReply(conn, reply)
}

func ReplyStrArray(conn net.Conn, data []string) error {
	if data == nil {
		reply := []byte("*-1\r\n")
		return writeReply(conn, reply)
	}

	if len(data) == 0 {
		reply := []byte("*0\r\n")
		return writeReply(conn, reply)
	}

	rl := 15
	for _, v := range data {
		if len(v) == 0 {
			rl += 6
		} else {
			rl += 10
			rl += len(v)
		}
	}

	reply := make([]byte, 1, rl)
	reply[0] = MultiBulkReply
	reply = append(reply, strconv.FormatInt(int64(len(data)), 10)...)
	reply = append(reply, "\r\n"...)
	for _, v := range data {
		if len(v) == 0 {
			reply = append(reply, "$0\r\n\r\n"...)
		} else {
			reply = append(reply, BulkReply)
			reply = append(reply, strconv.FormatInt(int64(len(v)), 10)...)
			reply = append(reply, "\r\n"...)

			reply = append(reply, v...)
			reply = append(reply, "\r\n"...)
		}
	}

	return writeReply(conn, reply)
}

func writeReply(conn net.Conn, bytes []byte) error {
	_, e := conn.Write(bytes)
	if e != nil {
		return e
	}
	return nil
}
