package proto

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/chrisxrepo/goutils/pool"
	"github.com/chrisxrepo/goutils/utils"
)

type CmdType int

const (
	TypeNone CmdType = iota
	TypeInline
	TypeMultLine
)

type RedisCommand struct {
	ct    CmdType
	mbulk int
	bulk  int
	item  [][]byte
}

func NewRedisCommand() *RedisCommand {
	c := &RedisCommand{
		ct:    TypeNone,
		mbulk: 0,
		bulk:  -1,
		item:  make([][]byte, 0, 4),
	}

	return c
}

func (c *RedisCommand) ParseCommand(buf *pool.ByteBuffer) (bool, error) {
	if buf.Len() == 0 {
		return false, nil
	}

	b := buf.PickByte()
	if c.ct == TypeNone {
		if b == '*' {
			c.ct = TypeMultLine
		} else {
			c.ct = TypeInline
		}
	}

	if c.ct == TypeMultLine {
		return c.parseMultLineCommand(buf)
	} else {
		return c.parseInlineCommand(buf)
	}
}

func (c *RedisCommand) parseMultLineCommand(buf *pool.ByteBuffer) (bool, error) {
	if c.mbulk == 0 {
		line := buf.ReadLine("\r\n")
		if line == nil || len(line) == 0 {
			return false, nil
		}

		num, err := strconv.ParseUint(utils.Bytes2Str(line[1:]), 10, 16)
		if err != nil || num > 1024*64 {
			return false, errors.New("Protocol error: invalid multi bulk length")
		}

		c.mbulk = int(num)
	}

	for i := len(c.item); i < c.mbulk; i++ {
		if c.bulk < 0 {
			cline := buf.ReadLine("\r\n")
			if cline == nil || len(cline) == 0 {
				return false, nil
			}

			if cline[0] != '$' {
				return false, fmt.Errorf("Protocol error: expected '$', got '%c'", cline[0])
			}

			num, err := strconv.ParseUint(utils.Bytes2Str(cline[1:]), 10, 16)
			if err != nil || num > 1024*1024 {
				return false, errors.New("Protocol error: invalid bulk length")
			}

			c.bulk = int(num)
		}

		vline := buf.ReadLine("\r\n")
		if vline == nil {
			return false, nil
		}

		if len(vline) != c.bulk {
			return false, errors.New("Protocol error: invalid bulk length")
		}

		c.item = append(c.item, vline)
		c.bulk = -1
	}

	for _, v := range c.item {
		fmt.Println(string(v))
	}

	return true, nil
}

func (c *RedisCommand) parseInlineCommand(buf *pool.ByteBuffer) (bool, error) {
	line := buf.ReadLine("\r\n")
	if line == nil {
		return false, nil
	}

	if len(line) > 1024*64 {
		return false, errors.New("Protocol error: too big inline request")
	}

	pos, l := 0, len(line)
	for pos < l {
		/* skip blanks */
		for ; line[pos] == ' '; pos++ {
		}

		var cur []byte
		if line[pos] == '"' {
			pos++
			cur = make([]byte, 0)
			for pos < l {
				if l-pos >= 4 && line[pos] == '\\' && line[pos+1] == 'x' && isHexDigit(line[pos+2]) && isHexDigit(line[pos+3]) {
					cur = append(cur, byte(hexDigitToInt(line[pos+2])*16+hexDigitToInt(line[pos+3])))
					pos += 4
				} else if l-pos >= 2 && line[pos] == '\\' {
					switch line[pos+1] {
					case 'n':
						cur = append(cur, '\n')
					case 'r':
						cur = append(cur, '\r')
					case 't':
						cur = append(cur, '\t')
					case 'b':
						cur = append(cur, '\n')
					case 'a':
						cur = append(cur, '\a')
					default:
						cur = append(cur, line[pos+1])
					}
					pos += 2
				} else if line[pos] == '"' {
					pos++
					goto DONE
				} else {
					cur = append(cur, line[pos])
					pos++
				}
			}

		} else if line[pos] == '\'' {
			pos++
			cur = make([]byte, 0)
			for pos < l {
				if l-pos >= 2 && line[pos] == '\\' && line[pos+1] == '\'' {
					cur = append(cur, '\'')
					pos += 2
				} else if line[pos] == '\'' {
					pos++
					goto DONE
				} else {
					cur = append(cur, line[pos])
					pos++
				}
			}

		} else {
			index := bytes.IndexByte(line[pos:], ' ')
			if index == -1 {
				index = l
			} else {
				index = pos + index
			}
			cur = line[pos:index]
			pos = index + 1
			goto DONE
		}

		return false, errors.New("Protocol error: unbalanced quotes in request")

	DONE:
		c.item = append(c.item, cur)
	}

	return true, nil
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func hexDigitToInt(c byte) int {
	switch c {
	case '0':
		return 0
	case '1':
		return 1
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	case '6':
		return 6
	case '7':
		return 7
	case '8':
		return 8
	case '9':
		return 9
	case 'a', 'A':
		return 10
	case 'b', 'B':
		return 11
	case 'c', 'C':
		return 12
	case 'd', 'D':
		return 13
	case 'e', 'E':
		return 14
	case 'f', 'F':
		return 15
	default:
		return 0
	}
	return 0
}

func (c *RedisCommand) Status() bool {
	if c.ct == TypeMultLine {
		return len(c.item) == c.mbulk
	}

	if c.ct == TypeInline {
		return len(c.item) > 0
	}

	return false
}

func (c *RedisCommand) Cmd() []byte {
	if len(c.item) == 0 {
		return nil
	}

	return c.item[0]
}

func (c *RedisCommand) Reset() {
	c.ct = TypeNone
	c.item = c.item[:0]
	c.mbulk = 0
	c.bulk = -1
}
