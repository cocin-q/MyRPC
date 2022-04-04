package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

/*
	每个编解码器都需要实现的方法有：
	1. 构造函数
	2. Codec接口规定的方法
		- ReadHeader
		- ReadBody
		- Write
		- Close
	结构体都需要有：
	1. 链接实例
	2. 缓冲区
	3. 解码器
	4. 编码器
*/

// GobCodec 定义Gob的结构体
type GobCodec struct {
	conn io.ReadWriteCloser // 由构造函数传入，通常是通过TCP或者Unix建立socket时得到的链接实例
	buf  *bufio.Writer      // 为了防止阻塞而创建的带缓冲的writer
	dec  *gob.Decoder       // gob对应的解码器
	enc  *gob.Encoder       // gob对应的编码器
}

/*
	使用 buffer 来优化写入效率, 所以我们先写入到 buffer 中,
	然后我们再调用 buffer.Flush() 来将 buffer 中的全部内容写入到 conn 中, 从而优化效率.
	对于读则不需要这方面的考虑, 所以直接在 conn 中读内容即可.
*/

// NewGobCodec Gob编码的构造函数
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush() // 最后记得清空缓冲区
		if err != nil {
			_ = c.Close() // 出错要关闭连接
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header: ", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body: ", err)
		return err
	}
	return nil
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
