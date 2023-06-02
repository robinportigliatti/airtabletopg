package airtableapi

type View struct {
	ID              string   `json:"id"`
	Type            string   `json:"type"`
	Name            string   `json:"name"`
	VisibleFieldIds []string `json:"visibleFieldIds"`
}
