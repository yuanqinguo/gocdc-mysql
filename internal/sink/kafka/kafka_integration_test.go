//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/sink/kafka/ -tags=integration -count=1
//
// 镜像默认 confluentinc/confluent-local:7.5.0（模块要求 KRaft 版 confluent-local；
// cp-kafka/apache 镜像布局与模块启动脚本不兼容），可用 KAFKA_IMAGE 覆盖。
package kafka

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/kafka"

	"gocdc/internal/model"
)

func TestIntegrationKafkaSink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	image := envOr("KAFKA_IMAGE", "confluentinc/confluent-local:7.5.0")
	ctr, err := kafka.Run(ctx, image)
	if err != nil {
		t.Fatalf("启动 Kafka 容器: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(Options{
		Brokers: brokers, TopicPrefix: "cdc_", OrderBy: "row",
		Idempotent: true, Ack: "all", MaxMessageBytes: 65536,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// fixture 用带点库名（edu.mall.admin 是库名，admin_user 是表）：回归统一命名
	// 下库名含点的折叠（点→下划线），默认 topic = cdc_edu_mall_admin_admin_user
	batch := []*model.Event{
		{EventID: "f:1:0", Type: model.EventInsert, Phase: "binlog", Database: "edu.mall.admin", Table: "admin_user",
			Ts: 1761624000, Pk: map[string]any{"id": int64(1)},
			After: map[string]any{"id": int64(1), "name": "a"}, SchemaVersion: 1,
			SourcePos: model.Position{Name: "f", Pos: 1}},
		{EventID: "f:2:0", Type: model.EventUpdate, Phase: "binlog", Database: "edu.mall.admin", Table: "admin_user",
			Ts: 1761624000, Pk: map[string]any{"id": int64(1)},
			Before: map[string]any{"id": int64(1), "name": "a"},
			After:  map[string]any{"id": int64(1), "name": "b"}, SchemaVersion: 1,
			SourcePos: model.Position{Name: "f", Pos: 2}},
	}
	// 建 topic（confluent-local 默认关自动创建）：容器内 kafka-topics 显式创建
	//（默认命名与 RocketMQ 统一：prefix + "{db}_{table}"，库名内点也替换）
	createTopic(t, ctx, ctr, "cdc_edu_mall_admin_admin_user")
	if err := s.Send(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// 重放（at-least-once 重复投递）
	if err := s.Send(ctx, batch); err != nil {
		t.Fatal(err)
	}

	// 消费回读（容器内 console consumer，验证 value 事件体）
	pctx, pcancel := context.WithTimeout(ctx, 60*time.Second)
	defer pcancel()
	exitCode, outReader, err := ctr.Exec(pctx, []string{"kafka-console-consumer",
		"--bootstrap-server", "localhost:9092", "--topic", "cdc_edu_mall_admin_admin_user",
		"--from-beginning", "--timeout-ms", "15000"})
	outB, _ := io.ReadAll(outReader)
	out := string(outB)
	_ = exitCode
	if err != nil && !strings.Contains(out, "Processed a total of 4") {
		t.Logf("console consumer: err=%v out=%s", err, out)
	}
	if n := strings.Count(out, `"event_id"`); n < 4 {
		t.Fatalf("消息体断言失败（应 ≥4 条含 event_id，实际 %d）: %s", n, out)
	}
	if !strings.Contains(out, `"type":"UPDATE"`) || !strings.Contains(out, `"type":"INSERT"`) {
		t.Fatalf("消息体缺 type: %s", out)
	}
}

func createTopic(t *testing.T, ctx context.Context, ctr *kafka.KafkaContainer, topic string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		out, _, err := ctr.Exec(ctx, []string{"kafka-topics", "--bootstrap-server", "localhost:9092",
			"--create", "--if-not-exists", "--topic", topic,
			"--partitions", "1", "--replication-factor", "1"})
		t.Logf("kafka-topics: err=%v out=%v", err, out)
		if err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("建 topic 超时")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
