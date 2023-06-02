package airtableapi

type AirtableResponse struct {
	Records []Record `json:"records"`
}
