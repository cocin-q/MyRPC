package MyRPC

import (
	"MyRPC/codec"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

// 客户端和服务端通信需要协商一些内容，服务端通过解析header就能够知道如何从body中读取需要的信息
// 对于本项目来说，唯一需要协商的就是消息的编解码方式
// 一般来说，涉及协议协商的这部分信息，需要固定的字节来传输。为了实现的方便，本项目采用Json编码Option
// 后续的header和body的编码方式由Option中的CodeType指定。所以服务端需要先Json解码Option，如何通过Option的CodecType解码剩余内容

/*
	| Option(Json) | Requese(Codec) |  --> | Option(Json) | Header(Codec) | Body(Codec) |
*/

const MagicNumber = 0x79779200
const defaultTimeout = time.Minute * 5 // 注册中心心跳超时时间

// Option 协商信息
type Option struct {
	MagicNumber    int           // 标记这是MyRPC的请求
	CodecType      codec.Type    // 客户端选择什么方式进行编码
	ConnectTimeout time.Duration // 连接超时 默认10s
	HandleTimeout  time.Duration // 处理超时 默认不设限 0s
}

// request 一个完整的请求，请求头，请求参数，响应
// 有服务注册以后，就得带上，哪个服务什么方法
type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// Accept 监听输入请求并提供服务，传入连接
func (server *Server) Accept(lis net.Listener) {
	for { // 循环等待socket连接建立 并开启子线程处理 处理过程交给ServerConn
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error :", err)
			return
		}
		go server.ServerConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// ServerConn 在本函数中主要是识别编解码的协商信息，然后调用进行具体的处理的函数
func (server *Server) ServerConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	// 协议协商
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 判断是不是发给本RPC的
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server : invalid magic number %x", opt.MagicNumber)
		return
	}
	// 获取对应的编解码格式 返回的是构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serverCodec(f(conn), &opt)
}

// invalidRequest 是发生错误时 argv 的占位符
var invalidRequest = struct{}{}

// serverCodec 三个阶段 明确了编解码的格式 开始具体的处理
// 1. 读取请求 readRequest  2. 处理请求 handleRequest  3. 回复请求 sendResponse
func (server *Server) serverCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // 处理请求是并发的，但是发送的时候得按顺序，不然可能会混淆数据
	wg := new(sync.WaitGroup)
	// 为什么这里是无限制循环 因为一次连接中允许接受多个请求，尽力而为，只有在header解析失败（可能所有请求结束了），才终止循环
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending) // 出错向客户端返回错误信息
			continue
		}
		wg.Add(1)
		// 把请求信息传入，处理请求 这里的这个timeout要注意，这里我们写死了，以后来改
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// readRequestHeader 读取请求头
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error: ", err)
		}
		return nil, err
	}
	return &h, nil
}

// readRequest 读取请求，先读取请求头，再读取请求体
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// reflect.TypeOf 获取对应的Type
	// reflect.New 返回一个值，该值表示指向指定类型的新零值的指针,这里其实是设置成，指向string类型的指针

	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		// 返回一个指针
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err: ", err)
		return req, err
	}

	return req, nil
}

// sendResponse 回复
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	// 因为开启了子线程去处理，所以需要用锁机制确保对缓冲区的互斥写
	sending.Lock()
	defer sending.Unlock()
	// 回复信息，Write方法调用了gob包中的encode方法，
	// encode方法用到了一个我们在gob结构体中定义的bufio.Writer缓冲区，所以需要自己上锁
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error: ", err)
	}
}

// handleRequest 处理请求，带有超时处理 解决send超时和协程泄露问题
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	var ctx context.Context
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.TODO())
	} else {
		ctx, cancel = context.WithTimeout(context.TODO(), timeout)
		defer cancel()
	}

	go func(context context.Context) {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		cancel()
	}(ctx)

	select {
	case <-ctx.Done():
		if timeout != 0 {
			req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
			//fmt.Println(req.h.Error)
			server.sendResponse(cc, req.h, invalidRequest, sending)
		}
	}
}

func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	// dup是true表示loaded
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// findService ServiceMethod 的构成是 “Service.Method”
// 先在serviceMap 中找到对应的 service 实例，再从 service 实例的 method 中，找到对应的 methodType。
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: server/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

//
// 支持HTTP协议。
// 先看将 HTTP 协议转换为 HTTPS 协议的过程：
// 1.浏览器向代理服务器发送CONNECT请求
// 2.代理服务器返回HTTP 200状态码表示连接已经建立
// 3.之后浏览器和服务器开始 HTTPS 握手并交换加密数据，代理服务器只负责传输彼此的数据包，并不能读取具体数据内容（代理服务器也可以选择安装可信根证书解密 HTTPS 报文）。
//
// 对 RPC 服务端来，需要做的是将 HTTP 协议转换为 RPC 协议，对客户端来说，需要新增通过 HTTP CONNECT 请求创建连接的逻辑。
// 1. 客户端向 RPC 服务器发送 CONNECT 请求
// 2. RPC 服务器返回 HTTP 200 状态码表示连接建立
// 3. 客户端使用创建好的连接发送 RPC 报文，先发送 Option，再发送 N 个请求报文，服务端处理 RPC 请求并响应。
//

/*
	此外go自带的rpc还提供rpc over tcp的选项，只需要在listen和dial时使用tcp连接就可以了。
	rpc over tcp和rpc over http 唯一的区别就是建立连接时的区别，
	实际的rpc over http也并没有使用http协议，只是用http server建立连接而已。
*/

const (
	connected        = "200 Connected to MyRPC"
	defaultRPCPath   = "/_myrpc_"
	defaultDebugPath = "/debug/myrpc"
)

// ServeHTTP 实现一个响应 RPC 请求的 http.Handler     ServeHTTP 应该将回复头和数据写入 ResponseWriter 然后返回。
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	// Hijack()可以将HTTP对应的TCP连接取出，连接在Hijack()之后，HTTP的相关操作就会受到影响，调用方需要负责去关闭连接。
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServerConn(conn)
}

// HandleHTTP 为rpcPath上的RPC消息注册一个HTTP处理程序
// 实际上HandleHTTP就是使用http包的功能，将server自身注册到http的url映射上了
func (server *Server) HandleHTTP() {
	// 第一个参数是访问路径  第二个参数是Handler类型 一个接口 需要实现ServerHTTP
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// HandleHTTP 默认服务器注册HTTP处理程序
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}

//
// 服务端向注册中心发送心跳信息
//

// Heartbeat 方法，便于服务启动时定时向注册中心发送心跳，默认周期比注册中心设置的过期时间少 1 min。
func (server *Server) Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		// time.NewTicker 创建周期性定时器
		t := time.NewTicker(duration)
		for err == nil {
			// 从定时器中获取数据
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// sendHeartbeat 发送心跳信息
func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Myrpc-Server", addr)
	// httpClient.Do 发送HTTP请求用的
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
