package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// 负载均衡策略
//
// 1. 随机选择策略 - 从服务列表中随机选择一个
// 2. 轮询算法(Round Robin) - 依次调度不同的服务器，每次调度执行 i = (i + 1) mode n。
// 3. 加权轮询(Weight Round Robin) - 在轮询算法的基础上，为每个服务实例设置一个权重，
//	  高性能的机器赋予更高的权重，也可以根据服务实例的当前的负载情况做动态的调整，例如考虑最近5分钟部署服务器的 CPU、内存消耗情况。
// 4. 哈希/一致性哈希策略 - 依据请求的某些特征，计算一个 hash 值，根据 hash 值将请求发送到对应的机器。
//	  一致性 hash 还可以解决服务实例动态添加情况下，调度抖动的问题。一致性哈希的一个典型应用场景是分布式缓存服务。

// 服务发现
// 负载均衡的前提是有多个服务实例，那我们首先实现一个最基础的服务发现模块 Discovery。
// 为了与通信部分解耦，这部分的代码统一放置在 xclient 子目录下

type SelectMode int // 代表不同负载均衡策略

const replicateCount = 5

const (
	RandomSelect     SelectMode = iota // 随机选择策略
	RoundRobinSelect                   // 轮询算法
	HashRingSelect                     // 一致性哈希算法
)

// Discovery 包含服务发现所需要的最基本的接口
type Discovery interface {
	Refresh() error                      // 从注册中心更新服务列表
	Update(servers []string) error       // 手动更新服务列表
	Get(mode SelectMode) (string, error) // 根据负载均衡策略，选择一个服务实例
	GetAll() ([]string, error)           // 返回所有的服务实例
}

// MultiServersDiscovery 实现一个不需要注册中心，服务列表由手工维护的服务发现的结构体
type MultiServersDiscovery struct {
	r       *rand.Rand   // 生成随机数
	mu      sync.RWMutex // 互斥访问控制
	servers []string     // 服务列表
	index   int          // 记录轮询算法已经选择的索引
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		// r 是一个产生随机数的实例，初始化时使用时间戳设定随机数种子，避免每次产生相同的随机数序列。
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		servers: servers,
	}
	// index 记录 Round Robin 算法已经轮询到的位置，为了避免每次从 0 开始，初始化时随机设定一个值。
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

// Refresh 刷新对 MultiServersDiscovery 没有意义，所以忽略它(因为他是手动维护的)
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update 更新服务列表
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
