package main

import (
	"context"
	"dytest/model"
	"errors"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func GetTableList(c *dynamodb.Client) ([]string, error) {
	out, err := c.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}
	return out.TableNames, nil
}

func CreateTable(c *dynamodb.Client, tableName string, input *dynamodb.CreateTableInput) error {
	var tableDesc *types.TableDescription

	table, err := c.CreateTable(context.TODO(), input)
	if err != nil {
		log.Printf("Failed to create table `%v` with error: %v\n", tableName, err)
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(c)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 5*time.Minute)
	if err != nil {
		log.Printf("Failed to wait on create table `%v` with error: %v\n", tableName, err)
		return err
	}

	tableDesc = table.TableDescription
	log.Printf("Created table `%s` with details: %v\n", tableName, tableDesc)
	return nil
}

func DeleteTable(c *dynamodb.Client, tableName string) error {
	_, err := c.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName)})
	if err != nil {
		log.Printf("Couldn't delete table %v. Here's why: %v\n", tableName, err)
	}
	return err
}

func SaveMovieItem(c *dynamodb.Client, movie model.MovieItem) error {
	av, err := attributevalue.MarshalMap(movie)
	if err != nil {
		log.Printf("Failed to marshal movie item: %v\n", err)
		return err
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("Movies"),
					Item:      av,
				},
			},
		},
	}

	_, err = c.TransactWriteItems(context.TODO(), input)
	if err != nil {
		log.Printf("Failed to transact write item: %v\n", err)
		return err
	}

	return nil
}

func GetMovieItem(c *dynamodb.Client, key map[string]types.AttributeValue) (model.MovieItem, error) {
	input := &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{
				Get: &types.Get{
					TableName: aws.String("Movies"),
					Key:       key,
				},
			},
		},
	}

	output, err := c.TransactGetItems(context.TODO(), input)
	if err != nil {
		log.Printf("Failed to transact get item: %v\n", err)
		return model.MovieItem{}, err
	}

	// Check if the response contains any items
	if len(output.Responses) == 0 || output.Responses[0].Item == nil {
		return model.MovieItem{}, errors.New("no item found")
	}

	// Unmarshal the item
	var movieItem model.MovieItem
	if err := attributevalue.UnmarshalMap(output.Responses[0].Item, &movieItem); err != nil {
		log.Printf("Failed to unmarshal movie item: %v\n", err)
		return model.MovieItem{}, err
	}

	return movieItem, nil
}

func ScanMovies(c *dynamodb.Client) ([]model.MovieItem, error) {
	// Create input parameters for Scan operation
	input := &dynamodb.ScanInput{
		TableName: aws.String("Movies"),
	}

	// Perform Scan operation
	result, err := c.Scan(context.Background(), input)
	if err != nil {
		log.Printf("Failed to scan items: %v\n", err)
		return nil, err
	}

	// Unmarshal scanned items
	var movies []model.MovieItem
	for _, item := range result.Items {
		var movie model.MovieItem
		if err := attributevalue.UnmarshalMap(item, &movie); err != nil {
			log.Printf("Failed to unmarshal movie item: %v\n", err)
			return nil, err
		}
		movies = append(movies, movie)
	}

	return movies, nil
}
