package MyRPC

import (
	"MyRPC/codec"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// go rpc中的对于服务端提供的方法的相关约束
// 1. 方法的类型必须是外部可见的
// 2. 方法必须是外部可见的
// 3. 方法参数只能有两个，而且必须是外部可见的类型或者是基本类型。
// 4. 方法的第二个参数类型必须是指针
// 5. 方法的返回值必须是error类型
// func (t *T) MethodName(argType T1, replyType *T2) error

// 导出，一个标志符被导出后就可以在其他包中使用，但是必须满足下面两个条件：
// 1.标识符的首字母是 Unicode 大写字母 (Unicode "Lu" 类); 而且
// 2.标识符要在包块中进行了声明，或是它是个字段名 /方法名。
// 而其他所有的标识符都不是导出的。

// DefaultOption 默认采用Gob编码方式
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Call 一次RPC调用需要的信息
type Call struct {
	Seq           uint64
	ServiceMethod string      // 需要调用的函数，格式是service.method
	Args          interface{} // 形参
	Reply         interface{} // 响应
	Error         error       // 错误信息
	Done          chan *Call  // 同步接口使用，结束标志
}

// done 为了支持同步调用，Call结构体中添加了一个字段Done，当调用结束时，会调用call.done()通知调用方
func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec      // 编码解码器，用来序列化将要发送出去的请求，以及反序列化接收到的响应
	opt      *Option          // 与服务端的协商信息
	header   codec.Header     // 请求的消息头，只有在请求发送的时候才需要，而请求发送是互斥的，因此每个客户端只需要一个，可复用
	pending  map[uint64]*Call // 存储未处理完的请求，键是编号，值是Call实例
	sending  sync.Mutex       // 保证请求的有序发送，防止出现多个请求报文混淆
	mu       sync.Mutex       // 客户端的互斥锁
	seq      uint64           // 给发送的请求编号，每个请求拥有唯一编号
	closing  bool             // 用户主动关闭
	shutdown bool             // 一般是有错误发送
}

// 判断Client是否实现了io.Closer接口
var _ io.Closer = (*Client)(nil)

// ErrShutdown errors.New 返回error类型的值 表示一个错误
var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 看客户端是否还在工作
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall 注册请求，将参数Call添加到client.pending中，并更新client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	// 注册请求，按照编号来
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// removeCall 根据seq从client.pending中移除对应的Call并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls 服务端或客户端发生错误时调用，将shutdown设置为true，且将错误信息通知所有pending状态的Call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// NewClient 创建Client实例，首先需要完成协议交换，然后再创建子线程调用receive()接收响应
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error: ", err)
		return nil, err
	}
	// 发送协议给服务端
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// newClientCodec 创建客户端，开始处理
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
		seq:     1, // 从1开始，0表示无效
	}
	go client.receive()
	return client
}

// parseOptions 用户确定协商信息，这里实现为可选参数，以便用户不设置可以默认
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

/*
	接收可能出现的情况：
	1. Call不存在，可能是请求没有发送完整，或者因为其他原因取消了，但是服务端仍旧处理了（客户端出问题）
	2. Call存在，服务端处理出错（服务端出问题）
	3. 正常
*/

// receive 接收响应
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil: // 客户端的Call列表中没有这个请求。可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
			err = client.cc.ReadBody(nil)
		case h.Error != "": // call存在，但服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default: // 正常情况
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body" + err.Error())
			}
			call.done()
		}
	}
	client.terminateCalls(err)
}

// send 发送请求
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册请求
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
	}

	// 准备请求头 因为互斥发送 客户端可以复用
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码和发送请求--请求头和请求体
	// 不是发送请求体吗？为什么只发送了参数		响应类型服务端自己能解析出来
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 返回调用的Call结构，没有阻塞，使其能够异步调用
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {		// call是对go的封装 实现同步调用，这个判断的话，似乎不满足同步调用
		log.Panic("rpc client : done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

//
// 超时处理
//

// 纵观整个远程调用的过程，需要客户端处理超时的地方有:
// 1. 与服务端建立连接，导致的超时
// 2. 发送请求到服务端，写报文导致的超时
// 3. 等待服务端处理时，等待处理导致的超时（比如服务端已挂死，迟迟不响应）
// 4. 从服务端接收响应时，读报文导致的超时

// 服务端处理超时的地方有：
// 1. 读取客户端请求报文时，读报文导致的超时
// 2. 发送响应报文时，写报文导致的超时
// 3. 调用映射服务的方法时，处理报文导致的超时

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(con net.Conn, opt *Option) (client *Client, err error)

// dialTimeout 能处理超时的连接请求：这里处理了两个超时问题，第一个是连接的时候超时，第二个是协议交换时候的超时
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	// 生成协商信息
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 连接超时处理
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// 出错，最后记得关闭连接
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)

	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	// select是对信道的操作，匹配的case随机选择一个执行，不匹配会阻塞，所以要注意select的超时处理
	// 协议交换超时处理
	select {
	case <-time.After(opt.ConnectTimeout): // 超时处理
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial 带有超时处理的连接请求 封装，向上屏蔽具体的连接过程
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// Call 带有超时处理，使用context包实现，控制权交给用户，控制更为灵活
// Call 调用对应的函数，等待完成，返回错误信息，阻塞call.Done，等待响应返回，是一个同步接口
// context主要就是用来在多个goroutine中设置截至日期，同步信号，传递请求相关值
// 他和WaitGroup的作用类似，但是更强大 https://www.cnblogs.com/failymao/p/15565326.html
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}, buffSize int) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, buffSize))		// 同步不应该没有缓冲区吗
	select {
	// 返回一个 channel，用于判断 context 是否结束，多次调用同一个 context done 方法会返回相同的 channel
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

//
// 客户端支持HTTP协议
//
// 支持 HTTP 协议的好处在于，RPC 服务仅仅使用了监听端口的 /_geerpc 路径，在其他路径上我们可以提供诸如日志、统计等更为丰富的功能。
//

// NewHTTPClient 创建通过HTTP连接的客户端
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// 需要获得HTTP正确的响应
	// ReadResponse 发送Request 从 bufio.NewReader(conn) 读取并返回一个 HTTP 响应
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{
		Method: "CONNECT",
	})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP 创建HTTP连接
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial 简化调用 提供一个统一入口XDial。rpcAddr是一个通用格式（protocol@addr）
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		return Dial(protocol, addr, opts...)
	}
}
