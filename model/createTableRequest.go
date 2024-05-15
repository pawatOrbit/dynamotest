package model

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

type CreateTableRequest struct {
	TableName            string                      `json:"table_name"`
	AttributeDefinitions []types.AttributeDefinition `json:"attribute_definitions"`
	KeySchema            []types.KeySchemaElement    `json:"key_schema"`
}
