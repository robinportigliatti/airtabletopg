package airtableapi

import (
	"fmt"
	"net/http"
)

type Base struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Airtable *Airtable // add this
	Tables	[]Table
	RelationshipInfos []RelationshipInfo
}

// Now the function can be a method of Base
func (b *Base) GetBaseSchema(a *Airtable) ([]Table, error) {
	path := fmt.Sprintf("https://api.airtable.com/v0/meta/bases/%s/tables", b.ID)
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b.Airtable.ApiKey))

	var result struct {
		Tables []Table `json:"tables"`
	}
	err = b.Airtable.do(req, &result)
	if err != nil {
		return nil, err
	}
	b.Tables = result.Tables
	return result.Tables, nil
}

func FindTableByName(tables []Table, name string) (*Table, bool) {
	for _, table := range tables {
		if table.Name == name {
			return &table, true
		}
	}
	return nil, false
}

func FindFieldByName(fields []Field, name string) (*Field, bool) {
	for _, field := range fields {
		if field.Name == name {
			return &field, true
		}
	}
	return nil, false
}

type BasesResponse struct {
	Bases []Base `json:"bases"`
}


