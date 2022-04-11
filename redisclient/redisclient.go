package redisclient

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"strconv"
	"time"
)

const (
	StreamName       = "StreamName"
	ConsumerGroup    = "ConsumerGroup"
	ConsumerUniqueId = "ConsumerUniqueId"
	StartReading     = "StartReading"
	BatchSize        = "BatchSize"
	Block            = "Block"
	NoAck            = "NoAck" //subcommand can be used to avoid adding the message to the PEL in cases where reliability is not a requirement
	ID               = "ID"
	MaxLen           = "MaxLen"
	Limit            = "Limit"
)

type ConsumerConf struct {
	StreamName       string
	ConsumerGroup    string
	ConsumerUniqueId string
	Start            string
	BatchSize        int64
	NoAck            bool
	Block            time.Duration
	End              string
	Count            int64
}

type Response struct {
	MessageID string
	Values    map[string]interface{}
}

func SyncConsumer(ctx context.Context, conf map[string]string, redisClient *redis.Client, handleNewEvent func(string, map[string]interface{}) error, createGroup bool) {
	log.Println(" Synchronous Consumer Group")
	cosumerConf := extractConf(conf)
	SyncConsumerWithConsumerConf(ctx, cosumerConf, redisClient, handleNewEvent, createGroup)

}

func AsyncConsumer(ctx context.Context, conf map[string]string, redisClient *redis.Client, consumerChanel chan<- Response, ackChanel <-chan string) {
	cosumerConf := extractConf(conf)
	go consumeEvents(ctx, redisClient, cosumerConf, consumerChanel)
	go asyncack(ctx, redisClient, cosumerConf, ackChanel)
}

func SyncConsumerWithConsumerConf(ctx context.Context, cosumerConf ConsumerConf, redisClient *redis.Client, handleNewEvent func(string, map[string]interface{}) error, createGroup bool) {
	if createGroup {
		var err = redisClient.XGroupCreate(ctx, cosumerConf.StreamName, cosumerConf.ConsumerGroup, cosumerConf.Start).Err()
		if err != nil {
			log.Println(err)
		}
	}
	for {
		entries, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    cosumerConf.ConsumerGroup,
			Consumer: cosumerConf.ConsumerUniqueId,
			Streams:  []string{cosumerConf.StreamName, cosumerConf.Start},
			Count:    cosumerConf.BatchSize,
			Block:    cosumerConf.Block,
			NoAck:    cosumerConf.NoAck,
		}).Result()
		if err != nil {
			log.Println(err)

		}
		for i := 0; i < len(entries[0].Messages); i++ {
			messageID := entries[0].Messages[i].ID
			values := entries[0].Messages[i].Values
			err := handleNewEvent(messageID, values)
			if err != nil {
				log.Printf("Not able to process the message %s. Error while processing the message %s", values, err)
			} else {
				redisClient.XAck(ctx, cosumerConf.StreamName, cosumerConf.ConsumerGroup, messageID).Result()
			}
		}
	}
}

func AsyncConsumerWithConsumerConf(ctx context.Context, conf ConsumerConf, redisClient *redis.Client, consumerChanel chan<- Response, ackChanel <-chan string) {
	go consumeEvents(ctx, redisClient, conf, consumerChanel)
	go asyncack(ctx, redisClient, conf, ackChanel)
}

func CreateConsumerGroup(ctx context.Context, conf map[string]string, redisClient *redis.Client) {
	stream := conf[StreamName]
	consumersGroup := conf[ConsumerGroup]
	start := conf[ConsumerUniqueId]
	error := redisClient.XGroupCreateMkStream(ctx, stream, consumersGroup, start).Err()
	if error != nil {
		log.Printf("Error while creating the consumer group  with stream %s ", error)
	}
}

func ReadPEL(ctx context.Context,
	conf ConsumerConf, redisClient *redis.Client) ([]redis.XPendingExt, error) {
	return redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   conf.StreamName,
		Group:    conf.ConsumerGroup,
		Start:    conf.Start,
		End:      conf.End,
		Count:    conf.Count,
		Consumer: conf.ConsumerUniqueId}).Result()
}

// TODO: publish event has to improve
func PublishEvent(ctx context.Context, conf map[string]string, client *redis.Client, data map[string]interface{}) error {
	stream := conf[StreamName]
	var id = conf[ID]
	maxLen, _ := strconv.Atoi(conf[MaxLen])
	limit, _ := strconv.Atoi(conf[Limit])
	var err = client.XAdd(ctx, &redis.XAddArgs{
		Stream:     stream,
		NoMkStream: true,
		MaxLen:     int64(maxLen),
		MinID:      "",
		Approx:     true,
		Limit:      int64(limit),
		ID:         id,
		Values:     data,
	}).Err()
	return err
}
func asyncack(ctx context.Context, redisClient *redis.Client, cosumerConf ConsumerConf, ackChanel <-chan string) {
	for {
		redisClient.XAck(ctx, cosumerConf.StreamName, cosumerConf.ConsumerGroup, <-ackChanel)
	}
}

func consumeEvents(ctx context.Context, redisClient *redis.Client, cosumerConf ConsumerConf, consumerChanel chan<- Response) {
	defer close(consumerChanel)
	for {
		entries, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    cosumerConf.ConsumerGroup,
			Consumer: cosumerConf.ConsumerUniqueId,
			Streams:  []string{cosumerConf.StreamName, cosumerConf.Start},
			Count:    cosumerConf.BatchSize,
			Block:    cosumerConf.Block,
			NoAck:    cosumerConf.NoAck,
		}).Result()
		if err != nil {
			log.Println(err)
			//return errors.New("Not able to init Redis client(XReadGroup) ", err)
		}

		for i := 0; i < len(entries[0].Messages); i++ {
			messageID := entries[0].Messages[i].ID
			values := entries[0].Messages[i].Values
			consumerChanel <- Response{messageID, values}
		}
	}
}

// Map is used for  backward compatibility.
func extractConf(conf map[string]string) ConsumerConf {
	stream := conf[StreamName]
	consumersGroup := conf[ConsumerGroup]
	uniqueID := conf[ConsumerUniqueId]
	start := conf[StartReading]
	batchSize, error := strconv.Atoi(conf[BatchSize])
	if error != nil {
		log.Println("Input batch size is not valid. Setting the default size to 10 !!")
		batchSize = 10
	}
	block, error := strconv.Atoi(conf[Block])
	if error != nil {
		block = 2000
		log.Printf("Input block is not valid. Setting the default size to 2000 !!")
	}
	noAck, error := strconv.ParseBool(conf[NoAck])
	if error != nil {
		noAck = false
		log.Printf("Inoput noAck is not valid. Setting the default size to false !!")
	}

	return ConsumerConf{StreamName: stream,
		ConsumerGroup: consumersGroup, ConsumerUniqueId: uniqueID, Start: start, BatchSize: int64(batchSize), NoAck: noAck, Block: time.Duration(block)}
}
