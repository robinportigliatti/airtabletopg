package postgres

import (
	"airtabletopg/airtable"
	"fmt"
	"strings"
	"regexp"

)

type Base struct {
	Id string
	Name string
	Tables []Table
	RelationshipInfos	[]*airtable.RelationshipInfo
}

// GetCreateDatabaseStatement generates a SQL statement to create a database
// with the name stored in the Base object
func (b *Base) GetCreateDatabaseStatement() string {
	return fmt.Sprintf("CREATE DATABASE %s;", NameFormat(b.Name))
}

func NameFormat(str string) string {
	name := strings.ToLower(str)
	name = strings.ReplaceAll(name, " ", "_")
	if str == "order" {
		name = "\"order\""
	}
	
	return name
}

func NameFormatJoin(str []string) []string {
	strs := []string{}
	for _, s := range str {
		strs = append(strs, NameFormat(s))
	}

	return strs
}
func Contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}


func (b *Base) PreDataSQL() []string {
	tableSQL := []string{}
	for _, table := range b.Tables {
		columnsSQL := []string{}
		for _, column := range table.Columns {
			relation, ok := airtable.FindFieldByIDInRelationShipInfos(b.RelationshipInfos, column.ID)
			if ok {
				if relation.RelationType == "ManyToOne" {
					relatedTable, _ := FindTableByID(b.Tables, relation.RelatedTable.ID)
					relatedTableColumnPK, _ := FindColumnByIDInAllTables(b.Tables, relatedTable.PrimaryFieldId)
					columnsSQL = append(columnsSQL, NameFormat(column.Name) + " " + ConvertToPostgresType(relatedTableColumnPK.Type))
				}
			} else {
				columnsSQL = append(columnsSQL, NameFormat(column.Name) + " " + ConvertToPostgresType(column.Type))
			}
			
		}
		tableSQL = append(tableSQL, "CREATE TABLE " + NameFormat(table.Name) + " (" + strings.Join(columnsSQL, ", ") + ");")
	}

	alreadyProcessed := []string{}

	for _, relation := range b.RelationshipInfos {
		// Dealing with ManyToOne correspond to FOREIGN KEY
		table, _ := FindTableByID(b.Tables, relation.Table.ID)
		// tableColumn, _ := FindColumnByIDInAllTables(b.Tables, relation.FieldID)
		tableColumnPK, _ := FindColumnByIDInAllTables(b.Tables, table.PrimaryFieldId)
		relatedTable, _ := FindTableByID(b.Tables, relation.RelatedTable.ID)
		relatedTableColumnPK, _ := FindColumnByIDInAllTables(b.Tables, relatedTable.PrimaryFieldId)

		if relation.RelationType == "ManyToMany" {
			if !Contains(alreadyProcessed, table.Name) && !Contains(alreadyProcessed, relatedTable.Name) {
				// Dealing With ManyToMany...
				// Le truc des ManyToMany c'est qu'il faut créer une table de correspondance
				// Elle aura pour nom <table_relatedTable>
				// CREATE TABLE %s_%s (id SERIAL PRIMARY KEY, %s %s, %s %s)
				relationSQL := fmt.Sprintf(
					"CREATE TABLE %s (id SERIAL PRIMARY KEY, %s %s, %s %s);",
					NameFormat(table.Name + "_" + relatedTable.Name),
					NameFormat(tableColumnPK.Name + "_" + table.Name),
					ConvertToPostgresType(tableColumnPK.Type),
					NameFormat(relatedTableColumnPK.Name + "_" + relatedTable.Name),
					ConvertToPostgresType(relatedTableColumnPK.Type))
				// Maintenant vu que le lien est entre les deux, il faut garder une historique que le lien a été fait
				tableSQL = append(tableSQL, relationSQL)
				alreadyProcessed = append(alreadyProcessed, table.Name)
				alreadyProcessed = append(alreadyProcessed, relatedTable.Name)
			}
		}
	}

	return tableSQL
}

func (b *Base) PostDataSQL() []string {
	tableSQL := []string{}

	alreadyProcessed := []string{}

	for _, relation := range b.RelationshipInfos {
		// Dealing with ManyToOne correspond to FOREIGN KEY
		table, _ := FindTableByID(b.Tables, relation.Table.ID)
		tableColumn, _ := FindColumnByIDInAllTables(b.Tables, relation.Field.ID)
		tableColumnPK, _ := FindColumnByIDInAllTables(b.Tables, table.PrimaryFieldId)
		relatedTable, _ := FindTableByID(b.Tables, relation.RelatedTable.ID)
		relatedTableColumnPK, _ := FindColumnByIDInAllTables(b.Tables, relatedTable.PrimaryFieldId)
		// FIXME: Doit être déportée dans un post-data
		if relation.RelationType == "ManyToOne" {
			// On récupère les informations de Table
			relationSQL := fmt.Sprintf(
				"ALTER %s ADD CONSTRAINT FOREIGN KEY (%s) REFERENCES %s (%s);",
				NameFormat(table.Name),
				NameFormat(tableColumn.Name),
				NameFormat(relatedTable.Name),
				NameFormat(relatedTableColumnPK.Name))
		
			tableSQL = append(tableSQL, relationSQL)
		// FIXME: Doit rester dans cette fonction
		} 
		if relation.RelationType == "ManyToMany" {
			if !Contains(alreadyProcessed, NameFormat(table.Name)) && !Contains(alreadyProcessed, NameFormat(relatedTable.Name)) {
				// Ici maintenant il faut faire les ALTER
				relationSQL := fmt.Sprintf(
					"ALTER TABLE %s ADD CONSTRAINT FOREIGN KEY (%s) REFERENCES %s (%s);",
					NameFormat(table.Name + "_" + relatedTable.Name),
					NameFormat(tableColumnPK.Name + "_" + table.Name),
					NameFormat(table.Name),
					NameFormat(tableColumnPK.Name))
				tableSQL = append(tableSQL, relationSQL)
				relationSQL = fmt.Sprintf(
					"ALTER TABLE %s ADD CONSTRAINT FOREIGN KEY (%s) REFERENCES %s (%s);",
					NameFormat(table.Name + "_" + relatedTable.Name),
					NameFormat(relatedTableColumnPK.Name + "_" + relatedTable.Name),
					NameFormat(relatedTable.Name),
					NameFormat(relatedTableColumnPK.Name))
				tableSQL = append(tableSQL, relationSQL)
				alreadyProcessed = append(alreadyProcessed, NameFormat(table.Name))
				alreadyProcessed = append(alreadyProcessed, NameFormat(relatedTable.Name))
			}
		}
	}

	return tableSQL
}


func MatchRegex(input, pattern string) (bool) {
	matched, err := regexp.MatchString(pattern, input)
	if err != nil {
		return false
	}
	return matched
}
