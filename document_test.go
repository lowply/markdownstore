package markdownstore

import (
	"bytes"
	"testing"
)

func TestSplitDocumentPreservesBody(t *testing.T) {
	data := []byte("---\r\nid: abc12345\r\n---\r\n\r\n  body\r\n\r\n")
	frontmatter, body, err := SplitDocument(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(frontmatter) != "id: abc12345\r\n" {
		t.Fatalf("frontmatter = %q", frontmatter)
	}
	if !bytes.Equal(body, []byte("  body\r\n\r\n")) {
		t.Fatalf("body = %q", body)
	}
}

func TestJoinDocumentPreservesMissingFinalNewline(t *testing.T) {
	got := JoinDocument([]byte("id: abc12345\n"), []byte("body"))
	want := []byte("---\nid: abc12345\n---\n\nbody")
	if !bytes.Equal(got, want) {
		t.Fatalf("document = %q, want %q", got, want)
	}
}
