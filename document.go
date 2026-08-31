package markdownstore

import (
	"bytes"
	"fmt"
)

func SplitDocument(data []byte) ([]byte, []byte, error) {
	line, next, ok := nextLine(data, 0)
	if !ok || string(line) != "---" {
		return nil, nil, fmt.Errorf("missing YAML frontmatter")
	}
	frontmatterStart := next
	for next <= len(data) {
		lineStart := next
		line, next, ok = nextLine(data, lineStart)
		if !ok {
			break
		}
		if string(line) != "---" {
			continue
		}
		bodyStart := next
		if bytes.HasPrefix(data[bodyStart:], []byte("\r\n")) {
			bodyStart += 2
		} else if bytes.HasPrefix(data[bodyStart:], []byte("\n")) {
			bodyStart++
		}
		return data[frontmatterStart:lineStart], data[bodyStart:], nil
	}
	return nil, nil, fmt.Errorf("unterminated YAML frontmatter")
}

func JoinDocument(frontmatter, body []byte) []byte {
	var output bytes.Buffer
	output.WriteString("---\n")
	output.Write(frontmatter)
	if len(frontmatter) > 0 && frontmatter[len(frontmatter)-1] != '\n' {
		output.WriteByte('\n')
	}
	output.WriteString("---\n\n")
	output.Write(body)
	return output.Bytes()
}

func nextLine(data []byte, start int) ([]byte, int, bool) {
	if start >= len(data) {
		return nil, start, false
	}
	end := bytes.IndexByte(data[start:], '\n')
	if end < 0 {
		return bytes.TrimSuffix(data[start:], []byte("\r")), len(data), true
	}
	end += start
	return bytes.TrimSuffix(data[start:end], []byte("\r")), end + 1, true
}
