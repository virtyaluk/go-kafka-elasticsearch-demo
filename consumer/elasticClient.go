package main

import (
	"context"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/olivere/elastic"
)

type elasticClient struct {
	ctx           *context.Context
	client        *elastic.Client
	bulkProcessor *elastic.BulkProcessor
}

func initElasticClient() *elasticClient {
	logger.Println("Initializing elasticsearch client")

	// Creating elasticsearch client instance
	ctx := context.Background()
	esClient, err := elastic.NewClient(
		elastic.SetURL(*esHost),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC", log.LstdFlags)),
		elastic.SetInfoLog(logger))

	if err != nil {
		printErrorAndExit(69, "Failed to start elasticsearch client: %s", err)
	}

	// Pinging elasticsearch host
	info, code, err := esClient.Ping(*esHost).Do(ctx)

	if err != nil {
		printErrorAndExit(69, "Failed to ping elasticsearch: %s", err)
	}

	logger.Println("Elasticsearch returned with code", code, "and version", info.Version.Number)

	// Checking whether elasticsearch index for user exists
	exists, err := esClient.IndexExists(*esIndex).Do(ctx)

	if err != nil {
		printErrorAndExit(69, "Failed to check index status: %s", err)
	}

	// If no, create one by reading mappings from the file
	if !exists {
		mapping, err := ioutil.ReadFile("user_mapping.json")

		if err != nil {
			printErrorAndExit(69, "Failed to read mapping file: %s", err)
		}

		_, err = esClient.CreateIndex(*esIndex).BodyString(string(mapping)).Do(ctx)

		if err != nil {
			printErrorAndExit(69, "Failed to create index: %s", err)
		}
	}

	logger.Println("Elasticsearch client is ready for action")

	return &elasticClient{
		ctx:    &ctx,
		client: esClient,
	}
}

func (ec *elasticClient) spinUpBulkProcessorAndStatsMonitor() {
	// Creates BulkProcessor with given set of workers
	bulkProcess, err := ec.client.BulkProcessor().
		Name("UsersWorker-1").
		Workers(*bpWorkers).
		BulkActions(10000).
		FlushInterval(30 * time.Second).
		Stats(true).
		Do(*ec.ctx)

	if err != nil {
		printErrorAndExit(69, "Failed to create BulkProcessor: %v", err)
	}

	// Show up stats every second
	go func(bp *elastic.BulkProcessor) {
		for range time.Tick(1 * time.Second) {
			printStats(bp)
		}
	}(bulkProcess)

	ec.bulkProcessor = bulkProcess
}

func printStats(bp *elastic.BulkProcessor) {
	stats := bp.Stats()

	logger.Println("")
	logger.Println("================ STATS =================")
	logger.Println("Number of times flush has been invoked:", stats.Flushed)
	logger.Println("Number of times workers committed reqs:", stats.Committed)
	logger.Println("Number of requests indexed            :", stats.Indexed)
	logger.Println("Number of requests reported as created:", stats.Created)
	logger.Println("Number of requests reported as updated:", stats.Updated)
	logger.Println("Number of requests reported as success:", stats.Succeeded)
	logger.Println("Number of requests reported as failed :", stats.Failed)
	logger.Println("")
}

func (ec *elasticClient) addBulkRequest(message string) {
	// Adds new message to the BulkProcessor
	id := strconv.Itoa(int(rand.Int31()))
	req := elastic.NewBulkIndexRequest().Index(*esIndex).Type(*esType).Id(id).Doc(message)
	ec.bulkProcessor.Add(req)
}

func (ec *elasticClient) close() {
	ec.bulkProcessor.Close()
}
