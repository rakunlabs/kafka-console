package kafka

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
)

// maxDocBytes bounds one documentation file; a page, not a dump.
const maxDocBytes = 1 << 20

// TopicDoc is a topic's documentation page, rendered from Markdown.
type TopicDoc struct {
	// HTML is the rendered page. Raw HTML in the Markdown is dropped and
	// links are limited to safe schemes, so it can be shown as is.
	HTML     string    `json:"html"`
	Source   string    `json:"source"`
	File     string    `json:"file"`
	Modified time.Time `json:"modified"`
}

// ErrNoDocs means the cluster has no docs path configured.
var ErrNoDocs = errors.New("topic documentation is not configured for this cluster")

// HasDocs reports whether topic documentation is configured.
func (c *Client) HasDocs() bool { return c.cfg.Docs != nil }

// TopicDoc reads <docs.path>/<topic>.md. A topic without a file is
// ErrNotFound, which the page shows as "no documentation yet".
func (c *Client) TopicDoc(topic string) (*TopicDoc, error) {
	if c.cfg.Docs == nil {
		return nil, ErrNoDocs
	}

	// Topic names are [a-zA-Z0-9._-]; anything else (a path) is refused
	// before it reaches the filesystem.
	if !validTopicName(topic) {
		return nil, ErrInvalid{fmt.Errorf("invalid topic name %q", topic)}
	}

	root, err := os.OpenRoot(c.cfg.Docs.Path)
	if err != nil {
		return nil, fmt.Errorf("docs; %w", err)
	}
	defer root.Close()

	name := topic + ".md"

	f, err := root.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("documentation for %q; %w", topic, ErrNotFound)
	}

	if err != nil {
		return nil, fmt.Errorf("docs; %w", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("docs; %w", err)
	}

	if st.Size() > maxDocBytes {
		return nil, ErrInvalid{fmt.Errorf("%s is %d bytes; documentation files are limited to %d", name, st.Size(), maxDocBytes)}
	}

	src, err := io.ReadAll(io.LimitReader(f, maxDocBytes))
	if err != nil {
		return nil, fmt.Errorf("docs; %w", err)
	}

	return &TopicDoc{
		HTML:     renderMarkdown(src),
		Source:   string(src),
		File:     filepath.ToSlash(name),
		Modified: st.ModTime(),
	}, nil
}

func renderMarkdown(src []byte) string {
	p := parser.NewWithExtensions(parser.CommonExtensions | parser.AutoHeadingIDs | parser.NoEmptyLineBeforeBlock)

	r := html.NewRenderer(html.RendererOptions{
		// No raw HTML at all: the file is someone else's text shown inside
		// the console, so it gets no way to run script or restyle the page.
		Flags: html.SkipHTML | html.Safelink | html.NofollowLinks | html.NoreferrerLinks | html.NoopenerLinks | html.HrefTargetBlank,
	})

	return string(markdown.Render(p.Parse(src), r))
}

func validTopicName(t string) bool {
	if t == "" || t == "." || t == ".." || len(t) > 249 {
		return false
	}

	return strings.IndexFunc(t, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-')
	}) < 0
}
