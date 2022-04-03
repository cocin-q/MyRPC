package codec

import (
	"bufio"
	"encoding/json"
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

type JsonCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *json.Decoder
	enc  *json.Encoder
}

func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &JsonCodec{
		conn: conn,
		buf:  buf,
		dec:  json.NewDecoder(conn),
		enc:  json.NewEncoder(buf),
	}
}

func (j *JsonCodec) ReadHeader(h *Header) error {
	return j.dec.Decode(h)
}

func (j *JsonCodec) ReadBody(body interface{}) error {
	return j.dec.Decode(body)
}

func (j *JsonCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = j.buf.Flush() // 最后记得清空缓冲区
		if err != nil {
			_ = j.Close() // 出错要关闭连接
		}
	}()
	if err := j.enc.Encode(h); err != nil {
		log.Println("rpc codec: json error encoding header: ", err)
		return err
	}
	if err := j.enc.Encode(body); err != nil {
		log.Println("rpc codec: json error encoding body: ", err)
		return err
	}
	return nil
}

func (j *JsonCodec) Close() error {
	return j.conn.Close()
}
