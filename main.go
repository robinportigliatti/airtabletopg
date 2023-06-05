package main

import (
	"github.com/mehanizm/airtable"
	a "airtabletopg/airtable"
	"airtabletopg/postgres"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

func exportTableToCSV(client *airtable.Client, b *airtable.Base, r []*a.RelationshipInfo, tableName string) error {
//	fmt.Printf("Processing table: %s\n", tableName)
	// Get schema and current table
	schema, err := client.GetBaseSchema(b.ID).Do()
	currentTable, _ := a.FindTableByName(schema.Tables, tableName)
	table := client.GetTable(b.ID, tableName)
	records,_ := table.GetRecords().Do()


	fileName := fmt.Sprintf("%s.csv", tableName)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, record := range records.Records {
		row, err := processRecord(record, currentTable, r, client, b)
		if err != nil {
			return err
		}

		err = writer.Write(row)
		if err != nil {
			return err
		}
	}
	fmt.Println(len(records.Records))
	if len(records.Records) != 0 {

		var keys []string
		record := records.Records[0]
		for k := range record.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sql []string
		sql = append(sql, fmt.Sprintf("\\COPY %s (" + strings.Join(keys, ", ") + ") FROM '%s' DELIMITER ',' CSV;", tableName, fileName ))
		writeCOPYSQL(sql, "data.sql")
	}
	return nil
}

func processRecord(record *airtable.Record, currentTable *airtable.TableSchema, r []*a.RelationshipInfo, client *airtable.Client, b *airtable.Base) ([]string, error) {
	var row []string
	var keys []string
	for k := range record.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	for _, fieldName := range keys {
		fieldValue := record.Fields[fieldName]
		var appendBool = true
		var valueToStore = fieldValue
		var err = errors.New("")
		// Si le duo fieldName
		// Dans notre objet Base on a notre table courante et le field courant
		currentField, _ := a.FindFieldByName(currentTable.Fields, fieldName)
		if currentField.Type == "multipleRecordLinks" {
			appendBool, valueToStore, err = processField(client, b, r, currentTable, currentField, fieldValue)
			if err != nil {
				return nil, err
			}
		}
		// fmt.Println(currentField)
		if appendBool {
			row = append(row, fmt.Sprintf("%v", valueToStore))
		}
	}

	return row, nil

}

func processField(client *airtable.Client, b *airtable.Base, r []*a.RelationshipInfo, currentTable *airtable.TableSchema, currentField *airtable.Field, fieldValue interface{}) (bool, interface{}, error) {
	appendBool := true
	valueToStore := fieldValue

	currentRelationIfFound, _ := a.FindRelationByTableIDAndFieldID(r, currentTable.ID, currentField.ID)
	if currentRelationIfFound != nil {
		if currentRelationIfFound.RelationType == "OneToMany" || currentRelationIfFound.RelationType == "ManyToMany" {
			appendBool = false
		} else if currentRelationIfFound.RelationType == "ManyToOne" {
			fieldValueSlice, ok := fieldValue.([]interface{})
			if !ok {
				return false, nil, fmt.Errorf("fieldValue n'est pas un slice")
			}
			id, ok := fieldValueSlice[0].(string)
			if !ok {
				return false, nil, fmt.Errorf("cannot convert fieldValue to string")
			}

			var relatedTable = client.GetTable(b.ID, currentRelationIfFound.RelatedTable.Name)
			currentRecord, err := relatedTable.GetRecord(id)
			if err != nil {
				return false, nil, err
			}
			
			primaryField, _ := a.FindFieldByID(&currentRelationIfFound.RelatedTable, currentRelationIfFound.RelatedTable.PrimaryFieldID)

			valueToStore = currentRecord.Fields[primaryField.Name]
		}
	}

	return appendBool, valueToStore, nil
}


func writeSQL(lines []string, fileName string) error {
	// Créer le fichier
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// Parcourir chaque ligne et l'écrire dans le fichier
	for _, str := range lines {
		_, err := file.WriteString(str + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func writeCOPYSQL(lines []string, fileName string) error {
    // Ouvre le fichier en mode append, crée le fichier s'il n'existe pas, 
    // et ouvre le fichier en mode écriture seulement
    file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    // Parcourir chaque ligne et l'écrire dans le fichier
    for _, str := range lines {
        _, err := file.WriteString(str + "\n")
        if err != nil {
            return err
        }
    }

    return nil
}

func deleteFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		// Si une erreur se produit lors de la suppression du fichier
		fmt.Println("Erreur lors de la suppression du fichier", err)
	}
}

func main() {
	apiKey := flag.String("api-key", "", "API key for Airtable")
	dbName := flag.String("dbname", "", "Database name")
	preData := flag.Bool("pre-data", false, "Export pre-data, true by default")
	data := flag.Bool("data", false, "Export data, true by default")
	postData := flag.Bool("post-data", false, "Export post-data, true by default")

	flag.Parse()

	if *apiKey == "" || *dbName == "" {
		fmt.Println("Both --api-key and --dbname must be provided")
		return
	}
	client := airtable.NewClient(*apiKey)

	bases, err := client.GetBases().Do()

	if err != nil {
		fmt.Println("Error while fetching bases: ", err)
		return
	}

	for _, base := range bases.Bases {
		if *dbName == base.Name {
			postgresBase := postgres.Base{Name: base.Name}

			schema, err := client.GetBaseSchema(base.ID).Do()
			if err != nil {
				fmt.Println("Error while fetching tables: ", err)
				continue
			}

			for _, table := range schema.Tables {
				//fmt.Println("")
				//fmt.Println(table)
				postgresTable := postgres.Table{ID: table.ID, Name: table.Name, PrimaryFieldId: table.PrimaryFieldID}
				for _, field := range table.Fields {
					//fmt.Println(field)
					column := postgres.Column{ID: field.ID, Name: field.Name, Type: field.Type}
					postgresTable.Columns = append(postgresTable.Columns, column)
				}

				postgresBase.Tables = append(postgresBase.Tables, postgresTable)
			}
			postgresBase.RelationshipInfos = a.AnalyzeRelationships(schema.Tables)
			if !*preData && !*data && !*postData {
				*preData = true
				*data = true
				*postData = true
			}
			if *preData {
				lines := []string{}
				lines = append(lines, postgresBase.GetCreateDatabaseStatement())
				for _, line := range postgresBase.PreDataSQL() {
					lines = append(lines, line)
				}
				
				writeSQL(lines, "pre-data.sql")
			}
			if *data {
				deleteFile("data.sql")
				for _, table := range postgresBase.Tables {
					err := exportTableToCSV(client, base, postgresBase.RelationshipInfos, table.Name)
					if err != nil {
						fmt.Printf("Error processing table %s: %v\n", table, err)
					}
				}
			}
			if *postData {
				writeSQL(postgresBase.PostDataSQL(), "post-data.sql")
			}
		}
	}
}
