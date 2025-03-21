

gofunboost 框架


# 1.使用golang语言实现类似知名的 python funboost 那样的万能通用的分布式函数执行框架


# 2. 已知python funboost用法如下：
```python
from funboost import boost,BrokerEnum

@boost(queue_name='queue_test',broker_kind=BrokerEnum.REDIS,concurrent_num=100)
def add(x, y):
    return x + y

@boost(queue_name='queue_test2',broker_kind=BrokerEnum.REDIS,concurrent_num=50)
def fun(a):
    print(a)

if __name__ == '__main__':
    add.consume()
    fun.consume()
    for i in range (1000):
        add.push(i,1*2)
        fun.push(i)
```

# 3. 首期要兼容redis rabbimtq 内存  作为broker ，保留扩展任意消息队列作为broker的队列

# 4. 支持设置 broker类型 conn连接数量 并发数量  qps控频 函数重试次数  这些功能。


## 4.1 Consume 函数

Consume 函数里面是for循环 ConnNum次数go 协程调用 ConsumeUsingOneConn 函数，ConsumeUsingOneConn 函数里面是for循环，从消息队列拉取消息，丢给协程池去运行。
ConsumeUsingOneConn 函数里面如果拉取消息失败，每隔60秒要重新尝试新建连接，不要直接返回错误和退出。Consume不管遇到断网还是中间件崩溃了都不要退出。 

## 4.2 Push函数如果报错，要
Push函数如果报错，要重试3次重新连接中间件，每次间隔5秒，重试3次都报错，要记录日志。


# 5. 要求使用 zap并切割来记录日志

"go.uber.org/zap"
"go.uber.org/zap/zapcore"
"gopkg.in/natefinch/lumberjack.v2"



# 6. golang gofunboost包最终要达到如下使用方式

```golang



// 定义一个加法函数
func add(x, y int) int {
	result := x + y
	log.Printf("计算 %d + %d = %d\n", x, y, result)
	time.Sleep(1 * time.Second)
	return result
}

// 定义一个打印函数
func printValue(a interface{}) {
	fmt.Printf("打印值: %v\n", a)
}


Options := boost.BoostOptions{
        QueueName:     "queue_test2",
        CousumeFunc：  add,
        BrokerKind:    broker.REDIS,
        ConnNum:       5,
        ConcurrentNum: 50,
        QPSLimit:      500,
        MaxRetries:    3,
        BrokerConfig: broker.Config{
				BrokerUrl: "localhost:6379",
                BrokerTransportOptions :map[string]interface{}{
                	"special1":123,
                }
			},
    }

addBooster := NewBroker(Options) // 根据BrokerKind来创建具体中间件类型的broker，例如 RedisBroker 或RabbitMQBroker 等。

printValueBooster := NewBroker(boost.BoostOptions{
        QueueName:     "queue_test33",
        CousumeFunc：  printValue,
        BrokerKind:    broker.REDIS,
        ConnNum:       5, // 连接数,代表创建5个redis连接拉取数据，然后丢到协程池去运行。不要创建和协程并发数量同等的连接去拉取数据，那样会造成redis连接数暴增。
        ConcurrentNum: 20,
        QPSLimit:      500,
        MaxRetries:    3,
        BrokerConfig: broker.Config{
				BrokerUrl: "localhost:6379",
                BrokerTransportOptions: map[string]interface{}{ // 可以设置一些特殊的配置，例如redis的特殊配置
                	"special1":123,
                    "special2":"aaaa",
                }
			},
    })


addBooster.Consume() // 启动消费函数,永远循环来取消费
printValueBooster.Consume() // 启动消费函数,永远循环来取消费

addFunc.Push(1,2)
printValueFunc.Push("hello world")

for {time.Sleep(10*time.Second)}  // 阻塞进程，让所有协程池一直运行。
```