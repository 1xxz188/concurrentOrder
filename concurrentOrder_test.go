package concurrentOrder

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPushMsg(t *testing.T) {
	cmap.SHARD_COUNT = 256
	testMap := cmap.New()

	rand.Seed(time.Now().Unix())

	sendGoCnt := 100
	msgCnt := 1000

	var revCnt int32
	var sendCnt int32
	shouldRevCnt := int32(msgCnt * sendGoCnt)

	fn := func(key string, data interface{}) {
		oldV, ok := testMap.Get(key)
		if !ok {
			panic("not find key")
		}
		newVV := data.(int)
		oldVV := oldV.(int)
		if (oldVV + 1) != newVV {
			panic("(oldVV + 1)!= newVV")
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))
		testMap.Set(key, newVV)

		atomic.AddInt32(&revCnt, 1)
		v := atomic.LoadInt32(&revCnt)
		if key == "100" && newVV%100 == 0 {
			fmt.Println(newVV, v)
		}
	}
	entity, err := NewInstance(DefaultOptions(fn))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(sendGoCnt)
	beginChan := make(chan struct{})

	for i := 0; i < sendGoCnt; i++ {
		go func(idx int) {
			defer wg.Done()
			<-beginChan

			key := strconv.Itoa(idx + 1)
			testMap.Set(key, 0)

			for i := 0; i < msgCnt; i++ {
				atomic.AddInt32(&sendCnt, 1)
				err = PushMsg(entity, key, i+1)
				require.NoError(t, err) //Maybe MsgCapacity is too small
			}
		}(i)
	}
	time.Sleep(time.Millisecond * 10)
	close(beginChan)
	wg.Wait()
	overChan := make(chan struct{})
	go func() {
		for {
			if atomic.LoadInt32(&revCnt) != atomic.LoadInt32(&shouldRevCnt) {
				time.Sleep(time.Millisecond)
			} else {
				close(overChan)
				return
			}
		}
	}()

	<-overChan
	fmt.Println(atomic.LoadInt32(&revCnt), atomic.LoadInt32(&shouldRevCnt))
	//require.Equal(t, shouldRevCnt, atomic.LoadInt32(&revCnt))
	//wgMsg.Wait()
}
