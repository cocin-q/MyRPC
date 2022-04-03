package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

// 带有注册中心的服务发现

type MyRegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string        // 注册中心地址
	timeout    time.Duration // 服务列表的过期时间
	lastUpdate time.Time     // 代表最后从注册中心更新服务列表的时间，默认 10s 过期，即 10s 之后，需要从注册中心更新新的列表
}

const defaultUpdateTimeout = time.Second * 10

func NewMyRegistryDiscovery(registerAddr string, timeout time.Duration) *MyRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &MyRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

// Update 更新服务中心的服务列表
func (d *MyRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

// Refresh 刷新本地的服务列表
func (d *MyRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// 没超时
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Myrpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *MyRegistryDiscovery) Get(mode SelectMode) (string, error) {
	// 先确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *MyRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
