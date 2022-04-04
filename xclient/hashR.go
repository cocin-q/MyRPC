package xclient

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashRing struct {
	replicateCount int               // 每台服务所对应的节点数量（实际节点 + 虚拟节点）
	nodes          map[uint32]string // 键：节点哈希值 ， 值：服务器地址
	sortedNodes    []uint32          // 从小到大排序后的所有节点哈希值切片，可以认为这个就是 哈希环
}

func New(nodes []string, replicateCount int) *HashRing {
	hr := new(HashRing)
	hr.replicateCount = replicateCount
	hr.nodes = make(map[uint32]string)
	hr.sortedNodes = []uint32{}
	hr.addNodes(nodes)

	return hr
}

/*
 * 作用：在哈希环上添加单个服务器节点（包含虚拟节点）的方法
 * 入参：服务器地址
 */ // AddNode
func (hr *HashRing) AddNode(masterNode string) {

	// 为每台服务器生成数量为 replicateCount-1 个虚拟节点
	// 并将其与服务器的实际节点一同添加到哈希环中
	for i := 0; i < hr.replicateCount; i++ {
		// 获取节点的哈希值，其中节点的字符串为 i+address
		key := hr.hashKey(strconv.Itoa(i) + masterNode)
		// 设置该节点所对应的服务器（建立节点与服务器地址的映射）
		hr.nodes[key] = masterNode
		// 将节点的哈希值添加到哈希环中
		hr.sortedNodes = append(hr.sortedNodes, key)
	}

	// 按照值从大到小的排序函数
	sort.Slice(hr.sortedNodes, func(i, j int) bool {
		return hr.sortedNodes[i] < hr.sortedNodes[j]
	})
}

/*
 * 作用：添加多个服务器节点（包含虚拟节点）的方法
 * 入参：服务器地址集合
 */
func (hr *HashRing) addNodes(masterNodes []string) {
	if len(masterNodes) > 0 {
		for _, node := range masterNodes {
			// 调用 addNode 方法为每台服务器创建实际节点和虚拟节点并建立映射关系
			// 最后将创建好的节点添加到哈希环中
			hr.AddNode(node)
		}
	}
}

/*
 * 作用：从哈希环上移除单个服务器节点（包含虚拟节点）的方法
 * 入参：服务器地址
 */
func (hr *HashRing) removeNode(masterNode string) {

	// 移除时需要将服务器的实际节点和虚拟节点一同移除
	for i := 0; i < hr.replicateCount; i++ {
		// 计算节点的哈希值
		key := hr.hashKey(strconv.Itoa(i) + masterNode)
		// 移除映射关系
		delete(hr.nodes, key)
		// 从哈希环上移除实际节点和虚拟节点
		if success, index := hr.getIndexForKey(key); success {
			hr.sortedNodes = append(hr.sortedNodes[:index], hr.sortedNodes[index+1:]...)
		}
	}
}

// 遍历
func (hr *HashRing) getIndexForKey(key uint32) (bool, int) {

	index := -1
	success := false

	for i, v := range hr.sortedNodes {
		if v == key {
			index = i
			success = true
			break
		}
	}

	return success, index
}

/*
 * 作用：给定一个客户端地址获取应当处理其请求的服务器的地址
 * 入参：客户端地址
 * 返回：应当处理该客户端请求的服务器的地址
 */ //GetNode
func (hr *HashRing) GetNode(key string) string {

	// 环上没服务器
	if len(hr.nodes) == 0 {
		return ""
	}

	// 获取客户端地址的哈希值
	hashKey := hr.hashKey(key)
	nodes := hr.sortedNodes

	// 当客户端地址的哈希值大于服务器上所有节点的哈希值时默认交给首个节点处理
	masterNode := hr.nodes[nodes[0]]

	for _, node := range nodes {
		// 如果客户端地址的哈希值小于当前节点的哈希值
		// 说明客户端的请求应当由该节点所对应的服务器来进行处理（逆时针）
		if hashKey < node {
			masterNode = hr.nodes[node]
			break
		}
	}

	return masterNode
}

/*
 * 作用：哈希函数（这里使用 crc32 算法来实现，返回的是一个 uint32 整型）
 * 入参：节点或客户端地址
 * 返回：地址所对应的哈希值
 */
func (hr *HashRing) hashKey(key string) uint32 {
	scratch := []byte(key)
	return crc32.ChecksumIEEE(scratch)
}
