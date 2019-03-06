package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"

	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of borkers to connect").Default("localhost:9092").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("Users").String()
	esHost     = kingpin.Flag("esHost", "Elasticsearch host").Default("http://localhost:9200").String()
	esIndex    = kingpin.Flag("esIndex", "Elasticsearch index name").Default("users").String()
	esType     = kingpin.Flag("esType", "Elasticsearch type name").Default("user").String()
	bpWorkers  = kingpin.
			Flag("bpWorkers", "The number of workers that are able to receive bulk requests and eventually commit them to Elasticsearch").
			Default("2").Int()
	bpActions = kingpin.
			Flag("bpActions", "The number of requests that can be enqueued before the bulk processor decides to commit.").
			Default("10000").Int()
	verbose  = kingpin.Flag("verbose", "Whether to turn on sarama logging").Default("true").Bool()
	logger   = log.New(os.Stderr, "", log.LstdFlags)
	esClient *elasticClient
)

func main() {
	logger.Println("Starting consumer...")

	// Preparation
	numcpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numcpu)

	// Parse command line args
	kingpin.Parse()

	// Enable verbose logs if set to
	if *verbose {
		sarama.Logger = logger
	}

	// Spin up elasticsearch client and its BulkProcessor
	esClient = initElasticClient()

	esClient.spinUpBulkProcessorAndStatsMonitor()
	defer esClient.close()

	// Do Kafka stuff
	upAndRunKafkaConsumer()
}

func upAndRunKafkaConsumer() {
	// Getting start with sarama by configuring it
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Creating consumer instance
	consumer, err := sarama.NewConsumer(*brokerList, config)

	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	// Getting the list of all available partitions of the topic
	partitionsList, _ := consumer.Partitions(*topic)

	var (
		messages = make(chan *sarama.ConsumerMessage)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	// Listening to interrupt events in separate goroutine
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	// Consume all partitions of the topic into one channel
	for _, partition := range partitionsList {
		// Creting partition consumer instance
		partitionConsumer, err := consumer.ConsumePartition(*topic, partition, sarama.OffsetOldest)

		if err != nil {
			printErrorAndExit(69, "Failed to start partition consumer: %s", err)
		}

		// Close partition consumer in a separeate goroutine when it comes to it
		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(partitionConsumer)

		wg.Add(1)

		//Redirect all the messages of given partition to `messages` channel
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()

			for message := range pc.Messages() {
				messages <- message
			}
		}(partitionConsumer)
	}

	// Working with consumed messages
	go func() {

		for message := range messages {
			// Send new messages directly to the BulkProccessor
			esClient.addBulkRequest(string(message.Value))
		}

	}()

	wg.Wait()
	logger.Println("Done consuming topic ", *topic)
	close(messages)

	// Finishing up
	if err := consumer.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "Error: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}
