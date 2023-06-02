package main

import (
	"airtabletopg/airtableapi"
	"airtabletopg/postgres"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
)

func exportTableToCSV(b airtableapi.Base, tableName string) error {
//	fmt.Printf("Processing table: %s\n", tableName)
	url := fmt.Sprintf("https://api.airtable.com/v0/%s/%s", b.ID, tableName)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Bearer "+b.Airtable.ApiKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var data airtableapi.AirtableResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}
	fileName := fmt.Sprintf("%s.csv", tableName)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	currentTable, _ := airtableapi.FindTableByName(b.Tables, tableName)

	for _, record := range data.Records {
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
			// Si le duo fieldName
			// Dans notre objet Base on a notre table courante et le field courant
			currentField, _ := airtableapi.FindFieldByName(currentTable.Fields, fieldName)
			if (currentField.Type == "multipleRecordLinks") {
				currentRelationIfFound, _ := airtableapi.FindRelationByTableIDAndFieldID(b.RelationshipInfos, currentTable.ID, currentField.ID)
				if currentRelationIfFound != nil {
					if currentRelationIfFound.RelationType == "OneToMany" {
						appendBool = false
					}
					if currentRelationIfFound.RelationType == "ManyToMany" {
						appendBool = false
					}
					if currentRelationIfFound.RelationType == "ManyToOne" {
						// Ici on récupère en fonction de la valeur le record de la table liée et du champs lié en fonction de l'identifiant
						// unique présent dans fieldValue
						//fmt.Println(fieldValue)
						fieldValueSlice, ok := fieldValue.([]interface{})
						if !ok {
							fmt.Println("fieldValue n'est pas un slice")
							continue
						}
						id, ok := fieldValueSlice[0].(string)

						currentRecord, _ := airtableapi.GetRecord(b.ID, currentRelationIfFound.RelatedTable.ID, id, b.Airtable.ApiKey)
						primaryField, _ := currentRelationIfFound.RelatedTable.FindFieldByID(currentRelationIfFound.RelatedTable.PrimaryFieldId)
						appendBool = false
						valueToStore = currentRecord.Fields[primaryField.Name]
					}
				}
			}
			// fmt.Println(currentField)
			if appendBool {
				row = append(row, fmt.Sprintf("%v", valueToStore))
			}
		}
	
		err = writer.Write(row)
		if err != nil {
			return err
		}
	}
	// FIXME: A la fin on écrit dans data.sql le COPY
	var keys []string
	record := data.Records[0]
	for k := range record.Fields {
		keys = append(keys, k)
		//fmt.Println(k)
	}
	sort.Strings(keys)
	
	fmt.Println(fmt.Sprintf("\\COPY %s (" + strings.Join(keys, ", ") + ") FROM '%s' DELIMITER ',' CSV;", tableName, fileName ))
	return nil
}


func writePreDataToCSV(lines []string, fileName string) error {
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
	
	airtable := airtableapi.Airtable{ApiKey: *apiKey}
	bases, err := airtable.GetBases()

	if err != nil {
		fmt.Println("Error while fetching bases: ", err)
		return
	}

	for _, base := range bases {
		if *dbName == base.Name {
			postgresBase := postgres.Base{Name: base.Name}

			tables, err := base.GetBaseSchema(&airtable)
			if err != nil {
				fmt.Println("Error while fetching tables: ", err)
				continue
			}

			for _, table := range tables {
				//fmt.Println("")
				//fmt.Println(table)
				postgresTable := postgres.Table{ID: table.ID, Name: table.Name, PrimaryFieldId: table.PrimaryFieldId}
				for _, field := range table.Fields {
					//fmt.Println(field)
					column := postgres.Column{ID: field.ID, Name: field.Name, Type: field.Type}
					postgresTable.Columns = append(postgresTable.Columns, column)
				}
				


				postgresBase.Tables = append(postgresBase.Tables, postgresTable)
			}
			postgresBase.RelationshipInfos = airtableapi.AnalyzeRelationships(tables)
			base.RelationshipInfos = postgresBase.RelationshipInfos
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
				
				writePreDataToCSV(lines, "pre-data.sql")
			}
			if *data {
				for _, table := range postgresBase.Tables {
					err := exportTableToCSV(base, table.Name)
					if err != nil {
						fmt.Printf("Error processing table %s: %v\n", table, err)
					}
				}
			}
			if *postData {
				writePreDataToCSV(postgresBase.PostDataSQL(), "post-data.sql")
			}
		}
	}
}
