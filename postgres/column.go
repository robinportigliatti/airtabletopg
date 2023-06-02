package postgres

type Column struct {
	ID string
	Name string
	Type string
}

func ConvertToPostgresType(airtableType string) string {
	switch airtableType {
	case "multilineText":
		return "TEXT"
	case "singleLineText":
		return "TEXT"
	case "number":
		return "INTEGER"
	case "dateTime":
		return "DATE"
	case "checkbox":
		return "BOOLEAN"
	case "Attachment":
		return "BYTEA"
	default:
		// Retourne le type par défaut si le type Airtable n'a pas été reconnu
		return "TEXT"
	}
}
