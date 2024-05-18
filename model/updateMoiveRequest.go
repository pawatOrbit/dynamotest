package model

type UpdateMovie struct {
	TableName                 string                 `json:"tableName"`
	Title                     string                 `json:"title"`
	Year                      int                    `json:"year"`
	UpdateExpression          string                 `json:"updateExpression"`
	ExpressionAttributeValues map[string]interface{} `json:"expressionAttributeValues"`
}