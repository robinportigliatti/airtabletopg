package airtableapi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type RecordResponse struct {
	ID          string                 `json:"id"`
	CreatedTime string                 `json:"createdTime"`
	Fields      map[string]interface{} `json:"fields"`
}

func GetRecord(baseId, tableIdOrName, recordId, apiKey string) (*RecordResponse, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.airtable.com/v0/%s/%s/%s", baseId, tableIdOrName, recordId), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("airtable API returned status %d: %s", resp.StatusCode, string(body))
	}

	recordResponse := &RecordResponse{}
	err = json.NewDecoder(resp.Body).Decode(recordResponse)
	if err != nil {
		return nil, err
	}

	return recordResponse, nil
}
