package codec

import "io"

// err = client.Call("Arith.Multiply", args, &reply)

// Header 请求和响应中的参数(args)和返回值(reply)放在body[这里用request结构体包括body了] 其余信息放在header
type Header struct {
	ServiceMethod string // 服务名.方法名
	Seq           uint64 // 请求的序号，用来区分不同的请求
	Error         string // 错误信息，客户端置为空，服务端如果发送错误，将信息存在Error中
}

// Codec 抽象出对消息体进行编码解码的接口 可屏蔽下面具体的编码方式 编解码器：主要是读写关闭
type Codec interface {
	io.Closer //io关闭的接口
	ReadHeader(header *Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// 定义编码解码的格式
// 这里定义了两种Codec，Gob和Json。实际代码只用了Gob

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	// 每种编码方式返回唯一的构造函数，这里放回的不是实例
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
