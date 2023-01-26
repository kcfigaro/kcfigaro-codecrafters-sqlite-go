package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	// Available if you need it!
	// "github.com/pingcap/parser"
	// "github.com/pingcap/parser/ast"
)

// B-tree Pages https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
type PageHeader struct {
	PageType            uint8
	FirstFreeBlockStart uint16
	NumberOfCells       uint16
	StartOfCell         uint16
	FragmentedFreeBytes uint8
}

// Storage Of The SQL Database Schema
type DbSchemaRow struct {
	_type    string
	name     string
	tblName  string
	rootPage int
	sql      string
}

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	var pageHeader PageHeader

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	switch command {
	case ".dbinfo":

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v\n", pageSize)

		pageHeader = parsePageHeader(databaseFile, pageHeader)

		// fmt.Printf("page type: %d\n", pageHeader.PageType)
		fmt.Printf("number of tables: %d\n", pageHeader.NumberOfCells)
		// fmt.Printf("start of the cell content area: %d\n", pageHeader.StartOfCell)

	case ".tables":
		_, _ = databaseFile.Seek(100, io.SeekStart)

		pageHeader = parsePageHeader(databaseFile, pageHeader)
		cellPointers := make([]uint16, pageHeader.NumberOfCells)

		for i := 0; i < int(pageHeader.NumberOfCells); i++ {
			cellPointers[i] = parseUInt16(databaseFile)
		}

		var dbschemarows []DbSchemaRow

		for _, cellPointer := range cellPointers {
			_, _ = databaseFile.Seek(int64(cellPointer), io.SeekStart)
			parseVarint(databaseFile) // number of bytes in payload
			parseVarint(databaseFile) // rowid
			record := parseRecord(databaseFile, 5)

			dbschemarows = append(dbschemarows, DbSchemaRow{
				_type:    string(record.values[0].([]byte)),
				name:     string(record.values[1].([]byte)),
				tblName:  string(record.values[2].([]byte)),
				rootPage: int(record.values[3].(uint8)),
				sql:      string(record.values[4].([]byte)),
			})
		}

		for i := 0; i < int(pageHeader.NumberOfCells); i++ {
			fmt.Printf("%v ", dbschemarows[i].tblName)
		}

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

// Extra code from https://github.com/codecrafters-io/languages/blob/master/starter_templates/sqlite/go/app/
func parsePageHeader(databaseFile *os.File, pageHeader PageHeader) PageHeader {
	leafPages := make([]byte, 8)

	_, err := databaseFile.Read(leafPages)
	if err != nil {
		log.Fatal(err)
	}

	err = binary.Read(bytes.NewReader(leafPages), binary.BigEndian, &pageHeader)
	if err != nil {
		fmt.Println("Failed to read integer:", err)
	}
	return pageHeader
}

func parseUInt8(stream io.Reader) uint8 {
	var result uint8

	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading uint8: %v", err)
	}

	return result
}

func parseUInt16(stream io.Reader) uint16 {
	var result uint16

	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading uint8: %v", err)
	}

	return result
}

type Record struct {
	values []interface{}
}

// parseRecord parses SQLite's "Record Format", as mentioned here: https://www.sqlite.org/fileformat.html#record_format
func parseRecord(stream io.Reader, valuesCount int) Record {
	parseVarint(stream) // number of bytes in header

	serialTypes := make([]int, valuesCount)

	for i := 0; i < valuesCount; i++ {
		serialTypes[i] = parseVarint(stream)
	}

	values := make([]interface{}, valuesCount)

	for i, serialType := range serialTypes {
		values[i] = parseRecordValue(stream, serialType)
	}

	return Record{values: values}
}

func parseRecordValue(stream io.Reader, serialType int) interface{} {
	if serialType >= 13 && serialType%2 == 1 {
		// Text encoding
		bytesCount := (serialType - 13) / 2
		value := make([]byte, bytesCount)
		_, _ = stream.Read(value)
		return value
	} else if serialType == 1 {
		// 8 bit twos-complement integer
		return parseUInt8(stream)
	} else {
		// There are more cases to handle, fill this in as you encounter them.
		log.Fatalf("Unknown serial type %v", serialType)
		return nil
	}
}

const IS_FIRST_BIT_ZERO_MASK = 0b10000000
const LAST_SEVEN_BITS_MASK = 0b01111111

func parseVarint(stream io.Reader) int {
	result := 0

	for index, usableByteAsInt := range readUsableBytesAsInts(stream) {
		var usableSize int

		if index == 8 {
			usableSize = 8
		} else {
			usableSize = 7
		}

		shifted := result << usableSize
		result = shifted + usableValue(usableSize, usableByteAsInt)
	}

	return result
}

func usableValue(usableSize int, usableByteAsInt int) int {
	if usableSize == 8 {
		return usableByteAsInt
	} else {
		return usableByteAsInt & LAST_SEVEN_BITS_MASK
	}
}

func readUsableBytesAsInts(stream io.Reader) []int {
	var usableBytesAsInts []int

	for i := 0; i < 9; i++ {
		byteAsInt := parseUInt8(stream)
		usableBytesAsInts = append(usableBytesAsInts, int(byteAsInt))

		if byteAsInt&IS_FIRST_BIT_ZERO_MASK == 0 {
			break
		}
	}

	return usableBytesAsInts
}
