package postgres

type Table struct {
	ID 		string
	Name    string
	PrimaryFieldId string
	Columns []Column
}

func FindTableByID(tables []Table, id string) (*Table, bool) {
	for _, table := range tables {
		if table.ID == id {
			return &table, true
		}
	}
	return nil, false
}

func FindColumnByIDInAllTables(tables []Table, id string) (*Column, bool) {
	for _, table := range tables {
		for _, column := range table.Columns {
			if column.ID == id {
				return &column, true
			}
		}
	}
	return nil, false
}