package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList   = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic        = kingpin.Flag("topic", "Topic name").Default("Users").String()
	maxRetry     = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
	verbose      = kingpin.Flag("verbose", "Whether to turn on sarama logging").Default("false").Bool()
	msgThreshold = kingpin.Flag("msgThreshold", "Max number of messages to produce in one go").Default("200000").Int()
	logger       = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	logger.Println("Starting producer...")

	// Preparations
	numcpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numcpu)

	// Parse command line args
	kingpin.Parse()

	// Enable verbose logs if set to
	if *verbose {
		sarama.Logger = logger
	}

	upAndRunKafkaProducer()
}

func upAndRunKafkaProducer() {
	// Getting start with sarama by configuring it
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = *maxRetry
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	// Creating new async producer
	producer, err := sarama.NewAsyncProducer(*brokerList, config)

	if err != nil {
		printErrorAndExit(69, "Failed to start producer: %s", err)
	}

	var (
		elapsed      time.Duration
		messagesSent int
	)

	// Defering stats to be printed out later on
	defer func() {
		logger.Println("Messages produced:", messagesSent)
		logger.Println("Elapsed:", elapsed)

		avgSpeed := int64(messagesSent) / int64(elapsed/time.Millisecond) * 1000

		logger.Println("Avg speed:", avgSpeed, "msg/sec")
	}()

	// Separate goroutine to count failed messages
	go func() {
		errors := 0

		defer logger.Println("Failed messages:", errors)

		for range producer.Errors() {
			errors++
		}
	}()

	// Actual stuff
	start := time.Now()

	for i := 0; i < *msgThreshold; i++ {
		person := GetRandomUserInfo()
		json, _ := json.Marshal(person)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.StringEncoder(json),
		}

		messagesSent++
	}

	elapsed = time.Since(start)

	// Finishing up
	if err := producer.Close(); err != nil {
		logger.Println("Failed to close producer:", err)
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "Error: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}
