package airtableapi

type Record struct {
	ID     string                 `json:"id"`
	Fields map[string]interface{} `json:"fields"`
}

