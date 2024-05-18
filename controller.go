// controller.go

package main

import (
	"dytest/model"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofiber/fiber/v2"
)

type Controller interface {
	GetTableList(c *fiber.Ctx) error
	CreateTable(c *fiber.Ctx) error
	DeleteTable(c *fiber.Ctx) error
	SaveMovieItem(c *fiber.Ctx) error
	GetMovieItem(c *fiber.Ctx) error
	ScanMovies(c *fiber.Ctx) error
	DeleteMovieItem(c *fiber.Ctx) error
	UpdateMovieItem(c *fiber.Ctx) error
}

type DynamoDBController struct {
	Client *dynamodb.Client
	Service *DynamoDBService
}

func (ctrl *DynamoDBController) GetTableList(c *fiber.Ctx) error {
	res, err := ctrl.Service.GetTableList(ctrl.Client)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(ErrorMessage{Error: err.Error()})
	}
	return c.JSON(res)
}

func (ctrl *DynamoDBController) CreateTable(c *fiber.Ctx) error {
	var requestBody model.CreateTableRequest

	if err := c.BodyParser(&requestBody); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body")
	}

	if requestBody.TableName == "" {
		return c.Status(http.StatusBadRequest).SendString("Table name is required")
	}
	tableInput := &dynamodb.CreateTableInput{
		AttributeDefinitions: requestBody.AttributeDefinitions,
		KeySchema:            requestBody.KeySchema,
		TableName:            aws.String(requestBody.TableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}

	err := CreateTable(ctrl.Client, requestBody.TableName, tableInput)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to create table: " + err.Error())
	}

	return c.Status(http.StatusCreated).SendString("Table created successfully")
}

func (ctrl *DynamoDBController) DeleteTable(c *fiber.Ctx) error {
	var requestBody model.DeleteTableRequest

	if err := c.BodyParser(&requestBody); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body")
	}

	if requestBody.TableName == "" {
		return c.Status(http.StatusBadRequest).SendString("Table name is required")
	}

	err := DeleteTable(ctrl.Client, requestBody.TableName)

	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to delete table: " + err.Error())
	}

	return c.Status(http.StatusOK).SendString("Table deleted successfully")
}

func (ctrl *DynamoDBController) SaveMovieItem(c *fiber.Ctx) error {
	var movie model.MovieItem

	if err := c.BodyParser(&movie); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body")
	}

	err := SaveMovieItem(ctrl.Client, movie)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to save movie item: " + err.Error())
	}

	return c.Status(http.StatusCreated).SendString("Movie item saved successfully")
}

func (ctrl *DynamoDBController) GetMovieItem(c *fiber.Ctx) error {

	type keys map[string]types.AttributeValue
	var movie model.MovieGetItem
	if err := c.BodyParser(&movie); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body!")
	}
	titleAttr, _ := attributevalue.Marshal(movie.Title)
	yearAttr, _ := attributevalue.Marshal(movie.Year)

	movieItem, err := GetMovieItem(ctrl.Client, keys{"title": titleAttr, "year": yearAttr})
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to get movie item: " + err.Error())
	}

	return c.JSON(movieItem)
}

func (ctrl *DynamoDBController) ScanMovies(c *fiber.Ctx) error {
	movies, err := ScanMovies(ctrl.Client)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to scan movies: " + err.Error())
	}
	return c.JSON(movies)
}

func (ctrl *DynamoDBController) DeleteMovieItem(c *fiber.Ctx) error {
	type keys map[string]types.AttributeValue

	var movie model.MovieGetItem
	if err := c.BodyParser(&movie); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body!")
	}
	titleAttr, _ := attributevalue.Marshal(movie.Title)
	yearAttr, _ := attributevalue.Marshal(movie.Year)
	err := DeleteMovieItem(ctrl.Client, keys{"title": titleAttr, "year": yearAttr})
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to delete movie item: " + err.Error())
	}
	return c.SendString("Movie item deleted successfully")
}

func (ctrl *DynamoDBController) UpdateMovieItem(c *fiber.Ctx) error {

	type keys map[string]types.AttributeValue
	var requestBody model.UpdateMovie

	if err := c.BodyParser(&requestBody); err != nil {
		return c.Status(http.StatusBadRequest).SendString("Invalid request body")
	}

	avMap, err_ := attributevalue.MarshalMap(requestBody.ExpressionAttributeValues)
	if err_ != nil {
		log.Fatalf("Failed to marshal map: %v", err_)
	}

	titleAttr, _ := attributevalue.Marshal(requestBody.Title)
	yearAttr, _ := attributevalue.Marshal(requestBody.Year)

	err := UpdateMovieItem(ctrl.Client, requestBody.TableName, keys{"title": titleAttr, "year": yearAttr}, requestBody.UpdateExpression, avMap)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to update movie item: " + err.Error())
	}

	return c.SendString("Movie item updated successfully")
}

