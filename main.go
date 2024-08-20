package main

import (
	"archive/zip"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetRow represents a single row in the combined Parquet file
type ParquetRow struct {
	NodeID         int64  `parquet:"name=node_id, type=INT64"`
	ParentNodeID   int64  `parquet:"name=parent_node_id, type=INT64, repetitiontype=OPTIONAL"`
	TagName        string `parquet:"name=tag_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	AttributeName  string `parquet:"name=attribute_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	AttributeValue string `parquet:"name=attribute_value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	IsNode         bool   `parquet:"name=is_node, type=BOOLEAN"`
	FilePath       string `parquet:"name=file_path, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// XMLNode is used to decode the XML structure
type XMLNode struct {
	XMLName xml.Name
	Content string     `xml:",chardata"`
	Attrs   []xml.Attr `xml:",any,attr"`
	Nodes   []XMLNode  `xml:",any"`
}

var nodeIDCounter int64 = 1

// parseXMLNode processes each XML node and writes the data to a Parquet file
func parseXMLNode(node XMLNode, parentNodeID int64, parquetWriter *writer.ParquetWriter, relativePath string) int64 {
	nodeID := nodeIDCounter
	nodeIDCounter++

	// Write the node itself
	row := ParquetRow{
		NodeID:       nodeID,
		ParentNodeID: parentNodeID,
		TagName:      node.XMLName.Local,
		IsNode:       true,
		FilePath:     relativePath,
	}
	if err := parquetWriter.Write(row); err != nil {
		log.Fatalf("Failed to write node: %v", err)
	}

	// Add the namespace as an attribute if present
	if node.XMLName.Space != "" {
		row := ParquetRow{
			NodeID:         nodeID,
			AttributeName:  "xmlns:" + node.XMLName.Space,
			AttributeValue: node.XMLName.Space,
			IsNode:         false,
			FilePath:       relativePath,
		}
		if err := parquetWriter.Write(row); err != nil {
			log.Fatalf("Failed to write attribute: %v", err)
		}
	}

	// Write the content as an attribute (if there's content)
	if node.Content != "" {
		trimmedContent := strings.TrimSpace(node.Content)
		if trimmedContent != "" {
			row := ParquetRow{
				NodeID:         nodeID,
				AttributeValue: trimmedContent,
				IsNode:         false,
				FilePath:       relativePath,
			}
			if err := parquetWriter.Write(row); err != nil {
				log.Fatalf("Failed to write attribute: %v", err)
			}
		}
	}

	// Write the other attributes
	for _, attr := range node.Attrs {
		row := ParquetRow{
			NodeID:         nodeID,
			AttributeName:  attr.Name.Local,
			AttributeValue: attr.Value,
			IsNode:         false,
			FilePath:       relativePath,
		}
		if err := parquetWriter.Write(row); err != nil {
			log.Fatalf("Failed to write attribute: %v", err)
		}
	}

	// Recursively process child nodes
	for _, childNode := range node.Nodes {
		parseXMLNode(childNode, nodeID, parquetWriter, relativePath)
	}

	return nodeID
}

// processXMLFile processes a single XML file and writes its data to the Parquet writer
func processXMLFile(fileName string, relativePath string, parquetWriter *writer.ParquetWriter) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open XML file %s: %v", fileName, err)
	}
	defer file.Close()

	decoder := xml.NewDecoder(file)

	var root XMLNode
	if err := decoder.Decode(&root); err != nil {
		return fmt.Errorf("failed to decode XML file %s: %v", fileName, err)
	}

	// Parse the XML and write to Parquet
	parseXMLNode(root, 0, parquetWriter, relativePath)

	return nil
}

// processFile processes a file based on its type
func processFile(fileName string, outputDir string, parquetWriter *writer.ParquetWriter, extensions []string) error {
	ext := strings.ToLower(filepath.Ext(fileName))

	relativePath, err := filepath.Rel(outputDir, fileName)
	if err != nil {
		return fmt.Errorf("failed to get relative path for file %s: %v", fileName, err)
	}

	for _, extension := range extensions {
		if ext == extension {
			dirPath := filepath.Dir(filepath.Join(outputDir, fileName))
			if _, err := os.Stat(dirPath); os.IsNotExist(err) {
				os.MkdirAll(dirPath, os.ModePerm)
			}

			err := processXMLFile(fileName, relativePath, parquetWriter)
			if err != nil {
				return err
			}

			// Check if directory is empty after processing
			if isEmptyDir(dirPath) {
				os.Remove(dirPath)
			}
			return nil
		}
	}

	if ext == ".zip" || ext == ".xlsx" || ext == ".docx" || ext == ".pptx" || ext == ".vsdx" || ext == ".odt" || ext == ".ods" || ext == ".odp" || ext == ".epub" || ext == ".apk" || ext == ".dtsx" || ext == ".csproj" || ext == ".vbproj" || ext == ".nuspec" || ext == ".plist" || ext == ".resx" || ext == ".dae" || ext == ".key" || ext == ".pages" || ext == ".numbers" {
		return extractAndProcessZip(fileName, outputDir, parquetWriter, extensions)
	}

	return copyNonXMLFile(fileName, outputDir)
}

// extractAndProcessZip extracts a ZIP file and processes XML files within it
func extractAndProcessZip(zipFile, outputDir string, parquetWriter *writer.ParquetWriter, extensions []string) error {
	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return fmt.Errorf("failed to open ZIP file %s: %v", zipFile, err)
	}
	defer r.Close()

	for _, f := range r.File {
		filePath := filepath.Join(outputDir, f.Name)

		if f.FileInfo().IsDir() {
			continue // Skip directories entirely
		}

		if strings.HasSuffix(f.Name, ".xml") || strings.HasSuffix(f.Name, ".rels") {
			dirPath := filepath.Dir(filePath)
			if _, err := os.Stat(dirPath); os.IsNotExist(err) {
				os.MkdirAll(dirPath, os.ModePerm)
			}

			rc, err := f.Open()
			if err != nil {
				return fmt.Errorf("failed to open file %s in ZIP: %v", f.Name, err)
			}

			tempFileName := filepath.Join(outputDir, f.Name)
			tempFile, err := os.Create(tempFileName)
			if err != nil {
				rc.Close()
				return fmt.Errorf("failed to create temp file for %s: %v", f.Name, err)
			}

			_, err = io.Copy(tempFile, rc)
			rc.Close()
			tempFile.Close()

			if err != nil {
				return fmt.Errorf("failed to copy contents of %s: %v", f.Name, err)
			}

			relativePath, err := filepath.Rel(outputDir, tempFileName)
			if err != nil {
				return fmt.Errorf("failed to get relative path for file %s: %v", tempFileName, err)
			}

			if err := processXMLFile(tempFileName, relativePath, parquetWriter); err != nil {
				return fmt.Errorf("failed to process XML file %s: %v", tempFileName, err)
			}

			os.Remove(tempFileName) // Clean up the temporary XML file

			// Check if directory is empty after processing
			if isEmptyDir(dirPath) {
				os.Remove(dirPath)
			}
		} else {
			dirPath := filepath.Dir(filePath)
			if _, err := os.Stat(dirPath); os.IsNotExist(err) {
				os.MkdirAll(dirPath, os.ModePerm)
			}
			dstFile, err := os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %v", filePath, err)
			}
			rc, err := f.Open()
			if err != nil {
				dstFile.Close()
				return fmt.Errorf("failed to open file %s in ZIP: %v", f.Name, err)
			}
			_, err = io.Copy(dstFile, rc)
			rc.Close()
			dstFile.Close()
			if err != nil {
				return fmt.Errorf("failed to copy file %s: %v", f.Name, err)
			}
		}
	}

	return nil
}

// isEmptyDir checks if a directory is empty
func isEmptyDir(dirPath string) bool {
	f, err := os.Open(dirPath)
	if err != nil {
		return false
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	return err == io.EOF
}

// cleanEmptyDirs recursively removes empty directories in the specified path
func cleanEmptyDirs(root string) error {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && isEmptyDir(path) {
			if err := os.Remove(path); err != nil {
				log.Printf("Failed to remove empty directory %s: %v", path, err)
			}
		}
		return nil
	})
	return err
}

// copyNonXMLFile copies non-XML files directly to the output directory
func copyNonXMLFile(fileName string, outputDir string) error {
	srcFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	defer srcFile.Close()

	dstFileName := filepath.Join(outputDir, filepath.Base(fileName))
	dstFile, err := os.Create(dstFileName)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", dstFileName, err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("failed to copy file %s: %v", fileName, err)
	}

	return nil
}

func main() {
	// Command-line flags
	extensionsFlag := flag.String("extensions", ".xml,.rels", "Comma-separated list of file extensions to parse")
	flag.Parse()

	if len(flag.Args()) != 2 {
		log.Fatalf("Usage: %s [--extensions=.ext1,.ext2] <file> <output-dir>", os.Args[0])
	}

	inputFile := flag.Arg(0)
	outputDir := flag.Arg(1)

	// Create the destination directory if it doesn't exist
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
			log.Fatalf("Failed to create output directory %s: %v", outputDir, err)
		}
	}

	// Parse the extensions
	extensions := strings.Split(*extensionsFlag, ",")
	for i, ext := range extensions {
		extensions[i] = strings.ToLower(strings.TrimSpace(ext))
	}

	// Initialize the single Parquet file writer
	parquetFileName := filepath.Join(outputDir, "combined.parquet")
	parquetFile, err := local.NewLocalFileWriter(parquetFileName)
	if err != nil {
		log.Fatalf("Failed to create Parquet file %s: %v", parquetFileName, err)
	}
	defer parquetFile.Close()

	parquetWriter, err := writer.NewParquetWriter(parquetFile, new(ParquetRow), 4)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}

	// Enable ZSTD compression
	parquetWriter.CompressionType = parquet.CompressionCodec_ZSTD
	defer parquetWriter.WriteStop()

	err = processFile(inputFile, outputDir, parquetWriter, extensions)
	if err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	// Clean up any remaining empty directories
	if err := cleanEmptyDirs(outputDir); err != nil {
		log.Fatalf("Failed to clean up empty directories: %v", err)
	}

	fmt.Println("Successfully processed file and generated Parquet file with ZSTD compression.")
}
