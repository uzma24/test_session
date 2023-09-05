package main

import (
	"context"
	"example/database"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-co-op/gocron"
	redis "github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	eventsource "gopkg.in/antage/eventsource.v1"
)

type Conf struct {
	ClientTimeOut int    `mapstructure:"DATA_TIMEOUT"`
	RedisURI      string `mapstructure:"REDIS_URI"`
}

type Data struct {
	Username string `json:"username"`
	Domain   string `json:"domain"`
	Name     string `json:"name"`
}
type SourceData struct {
	Timestamp time.Time `json:"timestamp"`
	Data      Data      `json:"data"`
}

func LoadConfig(path string) (config Conf, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}

func getDataSource(id int) []byte {
	url := "https://reqres.in/api/users/" + strconv.Itoa(id)

	// Send an HTTP GET request
	response, err := http.Get(url)
	if err != nil {
		fmt.Println("HTTP GET request error:", err)
		return nil
	}
	defer response.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil
	}
	return responseBody

}

func main() {

	//Load config
	config, err := LoadConfig(".")
	fmt.Println(config.ClientTimeOut)
	if err != nil {
		fmt.Print(err)
	}

	//connect to redis
	redisdb, err := database.ConnectRedis(config.RedisURI)
	if err != nil {
		log.Println("Error connecting to Redis DB!!")
	}

	ctx := context.Background()
	redisClient := redis.NewClient(redisdb)

	es := eventsource.New(nil, nil)
	defer es.Close()
	http.Handle("/events", es)
	go func() {
		id := 1
		for {
			//GET from REDIS
			dataJSON, err := redisClient.Get(ctx, strconv.Itoa(id)).Result()
			if err != nil {
				log.Println("Error getting data value from Redis:: ", err)
			}
			es.SendEventMessage(string(dataJSON), "tick-event", strconv.Itoa(id))
			id++
			time.Sleep(time.Duration(config.ClientTimeOut) * time.Second)
		}
	}()

	log.Fatal(http.ListenAndServe(":8080", nil))

	maxDataPoints := 50
	currDataPoints := 0
	id := 1
	// Create a new gocron scheduler instance
	scheduler := gocron.NewScheduler(time.UTC)

	// Create a channel to receive the data
	dataChannel := make(chan []byte)

	// Schedule the function to run every 10 seconds
	_, err = scheduler.Every(10).Seconds().Do(func() {
		data := getDataSource(id)
		dataChannel <- data // Send the data to the channel
	})
	if err != nil {
		fmt.Println("Error scheduling job:", err)
		return
	}

	// Start the scheduler
	scheduler.StartAsync()

	// Keep the program running to receive and handle data
	for {
		select {
		case data := <-dataChannel:
			currDataPoints++
			//SET data fetched from source into REDIS
			err = redisClient.Set(ctx, strconv.Itoa(id), data, time.Duration(time.Duration.Seconds(10))).Err()
			if err == redis.Nil {
				log.Println("Not able to set redis key!!", err)
			}
			if currDataPoints >= maxDataPoints {
				return
			}
		}
	}
}
