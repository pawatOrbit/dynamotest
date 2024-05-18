package test1

import (
	"context"
	dynamodbClient "dytest/dynamodb"
	"dytest/model"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofiber/fiber/v2"
)

type DynamoDBController2 struct {
	Client dynamodbClient.DynamodbClient
}

type ErrorMessage struct {
	Error string `json:"error"`
}

// func (cs *DynamoDBController2) GetTableList(c *fiber.Ctx) error {
// 	res, err := cs.Client.GetTableList(context.Background())

// 	if err != nil {
// 		c.Status(http.StatusInternalServerError).JSON(ErrorMessage{Error: err.Error()})
// 	}

// 	return c.JSON(res)
// }

func (cs *DynamoDBController2) SaveMovieItem(c *fiber.Ctx) error {
	var movie model.MovieItem

	if err := c.BodyParser(&movie); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body")
	}

	err := cs.Client.TransactWriteItems(context.Background(), "Movies", movie)

	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to save movie item: " + err.Error())
	}

	return c.Status(http.StatusCreated).SendString("Movie item saved successfully")
}

func (cs *DynamoDBController2) GetMovieItem(c *fiber.Ctx) error {

	type keys map[string]types.AttributeValue
	var movie model.MovieGetItem
	if err := c.BodyParser(&movie); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body!")
	}
	titleAttr, _ := attributevalue.Marshal(movie.Title)
	yearAttr, _ := attributevalue.Marshal(movie.Year)

	movieResult := &model.MovieGetItem2{}
	_, err := cs.Client.TransactGetItem(context.Background(), movie.TableName,  keys{"title": titleAttr, "year": yearAttr}, movieResult)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to get movie item: " + err.Error())
	}

	return c.JSON(movieResult)
}

func (cs *DynamoDBController2) ScanMovies(c *fiber.Ctx) error {
	movieResult := &[]model.MovieGetItem2{}
	_, err := cs.Client.Scan(context.Background(), "Movies2", movieResult)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to scan movies: " + err.Error())
	}
	return c.JSON(movieResult)
}

func (cs *DynamoDBController2) DeleteMovieItem(c *fiber.Ctx) error {
	type keys map[string]types.AttributeValue

	var movie model.MovieGetItem
	if err := c.BodyParser(&movie); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body!")
	}
	titleAttr, _ := attributevalue.Marshal(movie.Title)
	yearAttr, _ := attributevalue.Marshal(movie.Year)
	err := cs.Client.DeleteItem(context.Background(), movie.TableName, keys{"title": titleAttr, "year": yearAttr})
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to delete movie item: " + err.Error())
	}
	return c.SendString("Movie item deleted successfully")
}

func (cs *DynamoDBController2) UpdateMovieItem(c *fiber.Ctx) error {

	type keys map[string]types.AttributeValue
	var requestBody model.UpdateMovie

	if err := c.BodyParser(&requestBody); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body")
	}

	titleAttr, _ := attributevalue.Marshal(requestBody.Title)
	yearAttr, _ := attributevalue.Marshal(requestBody.Year)

	err := cs.Client.UpdateItem(context.Background(), requestBody.TableName, keys{"title": titleAttr, "year": yearAttr}, requestBody.UpdateExpression, requestBody.ExpressionAttributeValues)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to update movie item: " + err.Error())
	}

	return c.SendString("Movie item updated successfully")
}
