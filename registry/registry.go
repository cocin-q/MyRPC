package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// 实现注册中心
// 注册中心的好处在于，客户端和服务端都只需要感知注册中心的存在，而无需感知对方的存在。更具体一些:
//
// 1.服务端启动后，向注册中心发送注册消息，注册中心得知该服务已经启动，处于可用状态。一般来说，服务端还需要定期向注册中心发送心跳，证明自己还活着。
// 2.客户端向注册中心询问，当前哪天服务是可用的，注册中心将可用的服务列表返回客户端。
// 3.客户端根据注册中心得到的服务列表，选择其中一个发起调用。
//

type MyRegistry struct {
	timeout time.Duration //默认5分钟，任何注册的服务超过5分钟，都视为不可用
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_geerpc_/registry"
	defaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *MyRegistry {
	return &MyRegistry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultMyRegister = New(defaultTimeout)

// putServer 添加服务实例，如果服务已经存在，则更新start
func (r *MyRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{
			Addr:  addr,
			start: time.Now(),
		}
	} else {
		s.start = time.Now() // 更新时间，心跳信息
	}
}

// 给客户端返回可用的服务列表，如果存在超时的服务，则删除
func (r *MyRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// MyRegistry 采用HTTP协议
func (r *MyRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET": // 返回所有可用的服务列表
		w.Header().Set("X-Myrpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST": // 添加服务实例或发送心跳
		addr := req.Header.Get("X-Myrpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *MyRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultMyRegister.HandleHTTP(defaultPath)
}




