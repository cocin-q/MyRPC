package MyRPC

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

//
// 通过反射实现service -- 通过反射实现结构体和服务的映射关系
//

//
// 用到的反射知识实在是太多了，在这里补一下
// 1. 指针获取反射对象时，可以通过 reflect.Elem() 方法获取这个指针指向的元素类型，这个获取过程被称为取元素，等效于对指针类型变量做了一个*操作
// 2. 类型是结构体，可以通过反射值对象 reflect.Type 的 NumField() 和 Field() 方法获得结构体成员的详细信息
//		Field() 根据索引返回索引对应的结构体字段的信息		NumField() 返回结构体成员字段数量
//

// func (t *T) MethodName(argType T1, replyType *T2) error

type methodType struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 第一个参数的类型
	ReplyType reflect.Type   // 第二个参数的类型
	numCalls  uint64         // 统计方法调用次数
}

type service struct {
	name   string                 // 映射的结构体的名称
	typ    reflect.Type           // 结构体的类型
	rcvr   reflect.Value          // 结构体的实例本身
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法
}

func (m *methodType) NumCalls() uint64 {
	// func LoadUint64(addr *uint64) (val uint64) 传入一个指向uint64值的指针。然后返回加载到*addr的值
	return atomic.LoadUint64(&m.numCalls)
}

// newArgv 返回指向参数类型的零值指针，即创建对应类型的实例
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// Type是类型 Kind是类别 Kind范围更大
	if m.ArgType.Kind() == reflect.Ptr {
		// m.ArgType.Elem() 取指针类型的元素类型，即ArgType代表的那个参数是什么类型的
		// reflect.New 返回指向指定类型的零值的指针
		// 因为m.ArgType是指针，不是需要先获取具体的类型，然后返回执行具体类型零值的指针给argv
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// 因为m.ArgType不是指针类型 是值类型 想通过New获取对应零值的实例指针，然后通过指针指定类型的零值
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReplyv 创建对应类型的实例
func (m *methodType) newReplyv() reflect.Value {
	// Reply 一定是一个指针类型
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		// Set 把一个x(形参)中的数据赋值到v(调用者)
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

func newService(rcvr interface{}) *service {
	s := new(service)
	// 获得值的反射值对象,包含有rcvr的值信息
	s.rcvr = reflect.ValueOf(rcvr)
	// Indirect返回v指向的值，如果v是个nil指针，Indirect返回0值，如果v不是指针，Indirect返回v本身
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	// 通过检查抽象语法树，看对应名称的结构体是否是导出的（方法的类型是外部可见的）
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// 注册方法，实现结构体和服务的映射
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 符合条件的方法需要满足
		// 两个导出或内置类型的入参（反射时为 3 个，第 0 个是自身，类似于 python 的 self，java 中的 this）
		// 返回值有且只有 1 个，类型为 error
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s", s.name, method.Name)
	}
}

// isExportedOrBuiltinType 判断是否导出或者内置类型
func isExportedOrBuiltinType(t reflect.Type) bool {
	// PkgPath返回包名，代表这个包的唯一标识符，所以可能是单一的包名  包名为空 内置类型
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// call 实现通过反射值调用方法
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	// 传入参数，第一个是本身 类似Java的this，第二个是形参，第三个是响应值 最后返回函数运行结果error
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
