package model

type MovieGetItem struct {
	TableName string `json:"tableName"`
	Title     string `json:"title"`
	Year      int    `json:"year"`
}

type MovieGetItem2 struct {
	Title string                 `json:"title"`
	Year  int                    `json:"year"`
	Info  map[string]interface{} `json:"info"`
}

func (m *MovieGetItem2) TableName() string {
	return "Movies"
}
