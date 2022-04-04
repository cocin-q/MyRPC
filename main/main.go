package main

import (
	"MyRPC"
	"MyRPC/codec"
	"MyRPC/registry"
	"MyRPC/xclient"
	"context"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = args.Num1 + args.Num2
	return nil
}

// foo 封装一个方法 foo，便于在 Call 或 Broadcast 之后统一打印成功或失败的日志。
func foo(xc *xclient.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = xc.Broadcast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

func startRegistry(wg *sync.WaitGroup) {
	l, _ := net.Listen("tcp", ":9999")
	registry.HandleHTTP()
	wg.Done()
	_ = http.Serve(l, nil)
}

func startServer(registryAddr string, wg *sync.WaitGroup) {
	var foo Foo
	l, _ := net.Listen("tcp", ":0")
	server := MyRPC.NewServer()
	_ = server.Register(&foo)
	server.Heartbeat(registryAddr, "tcp@"+l.Addr().String(), 0)
	wg.Done()
	server.Accept(l)
}

func call(registry string, opt ...*MyRPC.Option) {
	d := xclient.NewMyRegistryDiscovery(registry, 0)
	xc := xclient.NewXClient(d, xclient.RandomSelect, opt[0])
	defer func() { _ = xc.Close() }()
	// send request & receive response
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func broadcast(registry string) {
	d := xclient.NewMyRegistryDiscovery(registry, 0)
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcast", "Foo.Sum", &Args{Num1: i, Num2: i * i})
			// expect 2 - 5 timeout
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func main() {

	var Option = &MyRPC.Option{
		MagicNumber:    MyRPC.MagicNumber,
		CodecType:      codec.JsonType,
		ConnectTimeout: time.Second * 10,
	}

	log.SetFlags(0)
	registryAddr := "http://localhost:9999/_geerpc_/registry"
	var wg sync.WaitGroup
	wg.Add(1)
	go startRegistry(&wg)
	wg.Wait()

	time.Sleep(time.Second)
	wg.Add(2)
	go startServer(registryAddr, &wg)
	go startServer(registryAddr, &wg)
	wg.Wait()

	time.Sleep(time.Second)
	call(registryAddr, Option)
	broadcast(registryAddr)
}

// 测试一致性哈希算法
//func main() {
//	server := []string{
//		"192.168.10.1",
//		"192.168.20.2",
//		"192.168.30.3",
//		"192.168.40.4",
//	}
//	hr := xclient.New(server, 10)
//
//	hr.AddNode("192.168.50.5")
//	fifth := 0
//	first, second, third, four := 0, 0, 0, 0
//	for i := 0; i < 100; i++ {
//		str := hr.GetNode(strconv.Itoa(i))
//		if strings.Compare(str, "192.168.10.1") == 0 {
//			fmt.Printf("192.168.1.1：%v \n", i)
//			first++
//		} else if strings.Compare(str, "192.168.20.2") == 0 {
//			fmt.Printf("192.168.2.2：%v \n", i)
//			second++
//		} else if strings.Compare(str, "192.168.30.3") == 0 {
//			fmt.Printf("192.168.3.3：%v \n", i)
//			third++
//		} else if strings.Compare(str, "192.168.40.4") == 0 {
//			fmt.Printf("192.168.4.4：%v \n", i)
//			four++
//		} else if strings.Compare(str, "192.168.50.5") == 0 {
//			fmt.Printf("192.168.5.5：%v \n", i)
//			fifth++
//		}
//	}
//
//	fmt.Printf("%v %v %v %v %v", first, second, third, four, fifth)
//
//}
