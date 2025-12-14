package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/commets", createCommet)
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil

}

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	fmt.Println(producer)

	return err

}

func createCommet(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	cmtbtInBytes, err := json.Marshal(cmt)
	PushCommentToQueue("comment", cmtbtInBytes)

	c.JSON(&fiber.Map{
		"success":  true,
		"messsage": "Comment pushed successfully",
		"comment":  cmt,
	})

	if err != nil {
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err

	}
	return err
}
