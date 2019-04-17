package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"github.com/satori/go.uuid"
	rejson "github.com/nitishm/go-rejson"
)

const (
	NanoSecondPrecision = 1000000000.0
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Wrapped    string      `json:"wrapped"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	//ProviderJobId  string  `json:"provider_job_id"`
	CreatedAt  float64     `json:"created_at"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	RetryCount int     `json:"retry_count,omitempty"`
	Retry      bool    `json:"retry,omitempty"`
	At         float64 `json:"at,omitempty"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func generateProviderJobId() string {
	u := uuid.NewV4()
	return fmt.Sprintf("%s", u)
}

func Enqueue(queue, class string, wrapped string, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, wrapped, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision()})
}

func EnqueueIn(queue, class string, wrapped string, in float64, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, wrapped, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision() + in})
}

func EnqueueAt(queue, class string, wrapped string, at time.Time, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, wrapped, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

func EnqueueWithOptions(queue, class string, wrapped string, args interface{}, opts EnqueueOptions) (string, error) {
	now := nowToSecondsWithNanoPrecision()

	//{"class":"ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper","wrapped":"UsappyUserInfoSyncToLocalJob","queue":"app_api_queue",
	//
	//
	//	"args":[{"job_class":"UsappyUserInfoSyncToLocalJob","job_id":"608256b2-08dd-4f02-8fd7-682c33915f04",
	//	"provider_job_id":null,"queue_name":"app_api_queue","priority":null,"arguments":[],"locale":"ja"}],
	//"retry":true,"jid":"702f891e99d768f517ed2dbb",
	//"created_at":1555412831.6633222,
	//"enqueued_at":1555412831.7229729}

	JobId := generateProviderJobId()
	Jid := generateJid()

	JsonArgs := []interface{}{ map[string]interface{}{ "job_class": wrapped, "job_id": JobId, "provider_job_id": nil, "queue_name": queue, "priority": nil, "arguments": args, "locale": "ja"  } }

	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Wrapped:        wrapped,
		Args:           JsonArgs,
		Jid:            Jid,
		EnqueuedAt:     now,
		CreatedAt:      now,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if now < opts.At {
		err := enqueueAt(data.At, bytes)
		return data.Jid, err
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("sadd", Config.Namespace+"queues", queue)
	if err != nil {
		return "", err
	}
	queue = Config.Namespace + "queue:" + queue
	_, err = conn.Do("rpush", queue, bytes)
	if err != nil {
		return "", err
	}

	StatusData := map[string]interface{}{ "worker": wrapped, "jid": Jid, "status": "queued", "update_time": now  }
	StatusDataBytes, err := json.Marshal(StatusData)
	if err != nil {
		return "", err
	}
	queue = Config.Namespace + "sidekiq:status:" + Jid
	_, err = rejson.JSONSet(conn, queue, ".", StatusDataBytes, false, false)
	if err != nil {
		return "", err
	}

	return data.Jid, nil
}

func enqueueAt(at float64, bytes []byte) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do(
		"zadd",
		Config.Namespace+SCHEDULED_JOBS_KEY, at, bytes,
	)
	if err != nil {
		return err
	}

	return nil
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func nowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}
