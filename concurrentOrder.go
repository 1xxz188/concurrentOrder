package concurrentOrder

import (
	"errors"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/yireyun/go-queue"
	"sync"
)

type Handle func(key string, data interface{})

type node struct {
	key string

	rwLock        sync.RWMutex
	isInReadyChan bool

	msgQueue *queue.EsQueue
}

type Options struct {
	nodeNum     int
	workNum     int
	oneCallCnt  int
	msgCapacity uint32
	fn          Handle
}

func DefaultOptions(fn Handle) Options {
	return Options{
		nodeNum:     10000,
		workNum:     200,
		oneCallCnt:  3,
		fn:          fn,
		msgCapacity: 1024,
	}
}
func (opt Options) WithWorkNum(workNum int) Options {
	opt.workNum = workNum
	return opt
}

func (opt Options) WithNodeNum(nodeNum int) Options {
	opt.nodeNum = nodeNum
	return opt
}

func (opt Options) WithMsgCapacity(msgCapacity uint32) Options {
	opt.msgCapacity = msgCapacity
	return opt
}

func (opt Options) WithOneCallCnt(oneCallCnt int) Options {
	opt.oneCallCnt = oneCallCnt
	return opt
}

type Instance struct {
	readyChan   chan *node
	nodeMap     cmap.ConcurrentMap
	fn          Handle
	nodeNum     int
	workNum     int
	oneCallCnt  int
	msgCapacity uint32
}

func NewInstance(opt Options) (*Instance, error) {
	if opt.nodeNum <= 0 {
		return nil, errors.New("concurrentOrder Init() nodeNum <= 0")
	}
	if opt.workNum <= 0 {
		return nil, errors.New("concurrentOrder Init() workNum <= 0")
	}
	if opt.oneCallCnt <= 0 {
		return nil, errors.New("concurrentOrder Init() oneCallCnt <= 0")
	}
	if opt.fn == nil {
		return nil, errors.New("concurrentOrder Init() opt.fn == nil")
	}

	cmap.SHARD_COUNT = 256
	instance := &Instance{
		readyChan:   make(chan *node, opt.nodeNum),
		nodeMap:     cmap.New(), //<ID, *node>
		fn:          opt.fn,
		nodeNum:     opt.nodeNum,
		workNum:     opt.workNum,
		oneCallCnt:  opt.oneCallCnt,
		msgCapacity: opt.msgCapacity,
	}

	for i := 0; i < instance.workNum; i++ {
		go func() {
			for item := range instance.readyChan {
				//only one coroutine call the unique ID at the same time
				var quantity uint32
				var ok bool
				var msg interface{}
				for i := 0; i < instance.oneCallCnt; i++ {
					msg, ok, quantity = item.msgQueue.Get()
					if !ok {
						break
					}
					instance.fn(item.key, msg)
					if quantity <= 0 {
						break
					}
				}
				if quantity <= 0 { //maybe still have value
					item.rwLock.Lock()
					if item.msgQueue.Quantity() > 0 {
						instance.readyChan <- item
					} else {
						item.isInReadyChan = false
					}
					item.rwLock.Unlock()
				} else { //have value, so push again
					instance.readyChan <- item
				}
			}
		}()
	}
	return instance, nil
}

func PushMsg(instance *Instance, key string, data interface{}) error {
	var item *node
	v, ok := instance.nodeMap.Get(key)
	if ok {
		item = v.(*node)
	} else {
		item = &node{
			key:      key,
			msgQueue: queue.NewQueue(instance.msgCapacity),
		}
		if ok := instance.nodeMap.SetIfAbsent(key, item); !ok {
			v, ok = instance.nodeMap.Get(key)
			if !ok {
				return errors.New("concurrentOrder PushMsg() not find key")
			}
			item = v.(*node)
		}
	}

	item.rwLock.Lock()
	defer item.rwLock.Unlock()

	ok, _ = item.msgQueue.Put(data)
	if !ok {
		return errors.New("concurrentOrder PushMsg() disable Put() key")
	}
	if item.isInReadyChan {
		return nil
	}
	item.isInReadyChan = true
	instance.readyChan <- item
	return nil
}
