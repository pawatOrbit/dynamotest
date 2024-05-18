package main

import (
	"context"
	dynamodbClient "dytest/dynamodb"
	"dytest/test1"
	"fmt"

	"github.com/gofiber/fiber/v2"
)

type ErrorMessage struct {
	Error string `json:"error"`
}

func main() {
	app := fiber.New()

	client, err := dynamodbClient.NewDynamodbClient(context.Background(),"your-profile-name")
	if err != nil {
		fmt.Println("Connection Error")
		return
	}

	controller := &test1.DynamoDBController2{Client: client}
	// app.Get("/get",controller.GetTableList)


	// controller := &DynamoDBController{Client: client}
	// app.Get("/get-table", controller.GetTableList)
	// app.Post("/create-table", controller.CreateTable)
	// app.Post("/delete-table", controller.DeleteTable)
	app.Post("/save-movie", controller.SaveMovieItem)
	app.Post("/get-movie", controller.GetMovieItem)
	app.Get("/scan-movies", controller.ScanMovies)
	app.Post("/delete-movie", controller.DeleteMovieItem)
	app.Post("/update-movie", controller.UpdateMovieItem)

	app.Listen(":3000")
}
