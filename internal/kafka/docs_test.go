package kafka

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rakunlabs/kafka-console/internal/config"
)

func TestTopicDoc(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()

	write := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write(filepath.Join(dir, "orders.v1.md"), "# Orders\n\nOwner: **team-a**\n\n<script>alert(1)</script>\n\n[x](javascript:alert(1)) [ok](https://example.com)\n\n| a | b |\n|---|---|\n| 1 | 2 |\n")
	write(filepath.Join(outside, "secret.md"), "secret")

	// A symlink inside the docs dir pointing out of it must not be followed.
	if err := os.Symlink(filepath.Join(outside, "secret.md"), filepath.Join(dir, "leak.md")); err != nil {
		t.Fatal(err)
	}

	c := &Client{cfg: &config.Cluster{Docs: &config.Docs{Path: dir}}}

	d, err := c.TopicDoc("orders.v1")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"<h1", "Orders", "<strong>team-a</strong>", "<table>", `href="https://example.com"`, `rel="nofollow noreferrer noopener"`} {
		if !strings.Contains(d.HTML, want) {
			t.Errorf("html lacks %q:\n%s", want, d.HTML)
		}
	}

	for _, bad := range []string{"<script", "javascript:"} {
		if strings.Contains(d.HTML, bad) {
			t.Errorf("html keeps %q:\n%s", bad, d.HTML)
		}
	}

	if _, err := c.TopicDoc("missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing file: %v", err)
	}

	for _, bad := range []string{"../secret", "a/b", "", "..", "x\x00"} {
		var invalid ErrInvalid
		if _, err := c.TopicDoc(bad); !errors.As(err, &invalid) {
			t.Errorf("topic %q: %v, want invalid", bad, err)
		}
	}

	if _, err := c.TopicDoc("leak"); err == nil {
		t.Error("followed a symlink out of the docs directory")
	}

	if _, err := (&Client{cfg: &config.Cluster{}}).TopicDoc("orders.v1"); !errors.Is(err, ErrNoDocs) {
		t.Errorf("no docs configured: %v", err)
	}
}
