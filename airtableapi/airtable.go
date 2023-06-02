package airtableapi

import (
	"net/http"
	"fmt"
	"encoding/json"
)

type Airtable struct {
	ApiKey string
}

func NewAirtable(apiKey string) *Airtable {
	return &Airtable{
		ApiKey: apiKey,
	}
}

func (a *Airtable) do(req *http.Request, v interface{}) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if v != nil {
		err = json.NewDecoder(resp.Body).Decode(v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Airtable) GetBases() ([]Base, error) {
	req, err := http.NewRequest("GET", "https://api.airtable.com/v0/meta/bases", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", a.ApiKey))

	var result struct {
		Bases []Base `json:"bases"`
	}
	err = a.do(req, &result)
	if err != nil {
		return nil, err
	}

	// Link each base to the airtable instance
	for i := range result.Bases {
		result.Bases[i].Airtable = a
	}

	return result.Bases, nil
}
