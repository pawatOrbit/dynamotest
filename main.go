package main

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
)

type ErrorMessage struct {
	Error string `json:"error"`
}

func main() {
	app := fiber.New()

	client, err := Newclient("your-profile-name")
	if err != nil {
		fmt.Println("Connection Error")
		return
	}
	controller := &DynamoDBController{Client: client}
	app.Get("/get-table", controller.GetTableList)
	app.Post("/create-table", controller.CreateTable)
	app.Post("/delete-table", controller.DeleteTable)
	app.Post("/save-movie", controller.SaveMovieItem)
	app.Post("/get-movie", controller.GetMovieItem)
	app.Get("/scan-movies", controller.ScanMovies)

	app.Listen(":3000")
}
