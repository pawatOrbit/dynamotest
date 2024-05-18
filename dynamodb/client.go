package dynamodbClient

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamodbClient interface {
	TransactGetItem(ctx context.Context, tableName string, key map[string]types.AttributeValue, result any) (*dynamodb.TransactGetItemsOutput, error)
	Scan(ctx context.Context, tableName string, result any) (*dynamodb.ScanOutput, error)
	TransactWriteItems(ctx context.Context, tableName string, body any) error
	DeleteItem(ctx context.Context, tableName string, key map[string]types.AttributeValue) error
	UpdateItem(ctx context.Context, tableName string, key map[string]types.AttributeValue, updateExpression string, requestBody any) error
}

type DynamodbClientImpl struct {
	serviceClient *dynamodb.Client
}

func NewDynamodbClient(ctx context.Context, profile string) (DynamodbClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("localhost"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil 
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "", SecretAccessKey: "", SessionToken: "",
				Source: "",
			},
		}), // IAM ROlE Connect append Middleware
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}
	c := dynamodb.NewFromConfig(cfg)

	finalDynamodbClient := &DynamodbClientImpl{
		serviceClient: c,
	}

	//Test connection with DynamoDB using TableList
	_, err = c.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		panic(fmt.Errorf("failed to connect to Dynamodb: %v", err))
	}
	return finalDynamodbClient, nil
}

// use case When atomicity and consistency across multiple items are required.
func (c *DynamodbClientImpl) TransactGetItem(ctx context.Context, tableName string, key map[string]types.AttributeValue, result any) (*dynamodb.TransactGetItemsOutput, error) {
	input := &dynamodb.TransactGetItemsInput{ //ReturnConsumedCapacity types.ReturnConsumedCapacity
		TransactItems: []types.TransactGetItem{
			{
				Get: &types.Get{
					TableName: aws.String(tableName),
					Key:       key,
				},
			},
		},
	}

	output, err := c.serviceClient.TransactGetItems(ctx, input)
	if err != nil {
		return nil, err
	}

	if err := attributevalue.UnmarshalMap(output.Responses[0].Item, &result); err != nil {
		return nil, err
	}

	return output, nil
}

// use case When you need to read every item in a table, often for reporting or bulk data operations.
func (c *DynamodbClientImpl) Scan(ctx context.Context, tableName string, result any) (*dynamodb.ScanOutput, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	output, err := c.serviceClient.Scan(ctx, input)
	if err != nil {
		return nil, err
	}

	if err := attributevalue.UnmarshalListOfMaps(output.Items, &result); err != nil {
		return nil, err
	}

	return output, nil
}

func (c *DynamodbClientImpl) TransactWriteItems(ctx context.Context, tableName string, body any) error {
	av, err := attributevalue.MarshalMap(body)
	if err != nil {
		return err
	}
	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String(tableName),
					Item:      av,
				},
			},
		},
	}

	_, err = c.serviceClient.TransactWriteItems(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func (c *DynamodbClientImpl) DeleteItem(ctx context.Context, tableName string, key map[string]types.AttributeValue) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	}
	_, err := c.serviceClient.DeleteItem(ctx, input)
	if err != nil {
		return err
	}
	return nil
}

func (c *DynamodbClientImpl) UpdateItem(ctx context.Context, tableName string, key map[string]types.AttributeValue, updateExpression string, requestBody any) error {
	expressionAttributeValues, err := attributevalue.MarshalMap(requestBody)
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ReturnValues:              types.ReturnValueUpdatedNew,
	}

	_, err = c.serviceClient.UpdateItem(ctx, input)
	if err != nil {
		return err
	}

	return nil
}
