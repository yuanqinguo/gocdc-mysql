// rmqcons 验证用 RocketMQ v5 消费端（gRPC SimpleConsumer）：
// 把收到的消息体逐行写入 JSONL，供对账脚本与源/目标 MySQL 比对。
//
//	go run . -topic CDC_rmqtest.msg -group verify-<ts> -out /tmp/rmq.jsonl
//
// 退出条件：空闲 idle 无消息（默认 10s）或达到 -max（默认 0=不限）。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:8081", "v5 gRPC proxy endpoint")
	topic := flag.String("topic", "", "topic")
	group := flag.String("group", "", "consumer group（全新组=从最新位点开始）")
	out := flag.String("out", "/tmp/rmq.jsonl", "输出 JSONL")
	idle := flag.Duration("idle", 10*time.Second, "空闲多久后退出")
	maxN := flag.Int("max", 0, "收到 N 条后退出（0=不限）")
	flag.Parse()
	if *topic == "" || *group == "" {
		fmt.Fprintln(os.Stderr, "-topic/-group 必填")
		os.Exit(2)
	}

	os.Setenv("mq.consoleAppender.enabled", "false")
	rmq_client.ResetLogger()

	c, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
		Endpoint:      *endpoint,
		ConsumerGroup: *group,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    "",
			AccessSecret: "",
		},
	},
		rmq_client.WithSimpleAwaitDuration(3*time.Second),
		rmq_client.WithSimpleSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			*topic: rmq_client.SUB_ALL,
		}),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "构造 consumer:", err)
		os.Exit(1)
	}
	if err := c.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "启动 consumer:", err)
		os.Exit(1)
	}
	defer c.GracefulStop()

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "创建输出:", err)
		os.Exit(1)
	}
	defer f.Close()

	ctx := context.Background()
	total := 0
	last := time.Now()
	for {
		mvs, err := c.Receive(ctx, 32, 20*time.Second)
		if err != nil {
			if time.Since(last) >= *idle {
				break
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, mv := range mvs {
			if _, werr := f.Write(append(mv.GetBody(), '\n')); werr != nil {
				fmt.Fprintln(os.Stderr, "写入:", werr)
				os.Exit(1)
			}
			_ = c.Ack(ctx, mv)
			total++
			last = time.Now()
		}
		if *maxN > 0 && total >= *maxN {
			break
		}
		if time.Since(last) >= *idle {
			break
		}
	}
	_ = f.Sync()
	fmt.Printf("consumed=%d out=%s\n", total, *out)
}
