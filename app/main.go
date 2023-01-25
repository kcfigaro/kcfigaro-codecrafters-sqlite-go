package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

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

		leafPages := make([]byte, 8)

		_, err = databaseFile.Read(leafPages)
		if err != nil {
			log.Fatal(err)
		}

		var pageHeader PageHeader
		err = binary.Read(bytes.NewReader(leafPages), binary.BigEndian, &pageHeader)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		fmt.Printf("number of tables: %d\n", pageHeader.NumberOfCells)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
