package logicdb

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestReaderWithPos(t *testing.T) {
	// Create a temporary file with some content
	content := "Hello, this is a test content."
	file, err := ioutil.TempFile("", "testfile.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	file.WriteString(content)
	file.Seek(0, io.SeekStart)
	// Create a ReaderWithPos using the temporary file
	reader := NewReaderWithPos(file)

	// Test Read method
	buf := make([]byte, 10)
	n, err := reader.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("Expected to read 10 bytes, but read %d", n)
	}
	if reader.pos != 10 {
		t.Fatalf("Expected position to be 10, but got %d", reader.pos)
	}

	// Test Seek method
	newPos, err := reader.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if newPos != 5 {
		t.Fatalf("Expected new position to be 5, but got %d", newPos)
	}
	if reader.pos != 5 {
		t.Fatalf("Expected position to be 5 after seek, but got %d", reader.pos)
	}

	// Test Seek method with io.SeekCurrent
	newPos, err = reader.Seek(3, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if newPos != 8 {
		t.Fatalf("Expected new position to be 8, but got %d", newPos)
	}
	if reader.pos != 8 {
		t.Fatalf("Expected position to be 8 after seek, but got %d", reader.pos)
	}
}

func TestWriterWithPos(t *testing.T) {
	// Create a temporary file
	file, err := ioutil.TempFile("", "testfile.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Create a WriterWithPos using the temporary file
	writer := NewWriterWithPos(file)

	// Test Write method
	content := []byte("Hello, this is a test content.")
	n, err := writer.Write(content)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(content) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(content), n)
	}
	if writer.pos != uint64(len(content)) {
		t.Fatalf("Expected position to be %d, but got %d", len(content), writer.pos)
	}

	// Test Seek method
	newPos, err := writer.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if newPos != 5 {
		t.Fatalf("Expected new position to be 5, but got %d", newPos)
	}
	if writer.pos != 5 {
		t.Fatalf("Expected position to be 5 after seek, but got %d", writer.pos)
	}

	// Test Seek method with io.SeekCurrent
	newPos, err = writer.Seek(3, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if newPos != 8 {
		t.Fatalf("Expected new position to be 8, but got %d", newPos)
	}
	if writer.pos != 8 {
		t.Fatalf("Expected position to be 8 after seek, but got %d", writer.pos)
	}

	// Verify content in the file
	file.Seek(0, io.SeekStart)
	readContent, _ := ioutil.ReadAll(file)
	if !strings.HasPrefix(string(readContent), "Hello, this is a test content.") {
		t.Fatalf("Unexpected content in the file: %s", readContent)
	}
}
