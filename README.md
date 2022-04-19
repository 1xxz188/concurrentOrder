# concurrentOrder
require sequential processing of certain things at high concurrency. Solve the head blocking problem.<br>
The same key keeps the callbacks called sequentially, and different keys can be called by concurrently.<br>

需要以高并发顺序处理某些事情，解决头部阻塞问题。<br>
相同的键保持回调顺序处理，不同的键可以同时调用。<br>

## Demo
```
func TestDemo(t *testing.T) {
        exitChan := make(chan struct{})
        
	fn := func(key string, data interface{}) {
		fmt.Println(key, data)
		close(exitChan)
	}
	
	entity, err := NewInstance(DefaultOptions(fn))
	require.NoError(t, err)

	err = PushMsg(entity, "key", "value")
	require.NoError(t, err)
	<-exitChan
}
```
