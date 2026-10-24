package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// requireRegistryWrite is a registry change: it needs a registry and a
// writable cluster.
func (c *Client) requireRegistryWrite() error {
	if err := c.requireRegistry(); err != nil {
		return err
	}

	return c.requireWritable()
}

func srErr(op string, err error) error {
	var re *sr.ResponseError
	if errors.As(err, &re) {
		switch re.StatusCode {
		case http.StatusNotFound:
			return fmt.Errorf("%s; %w: %s", op, ErrNotFound, re.Message)
		case http.StatusUnprocessableEntity, http.StatusConflict, http.StatusBadRequest:
			return ErrInvalid{fmt.Errorf("%s; %s", op, re.Error())}
		}
	}

	return fmt.Errorf("%s; %w", op, err)
}

type SubjectSummary struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	LatestVersion int    `json:"latest_version"`
	LatestID      int    `json:"latest_id"`
	Versions      int    `json:"versions"`
	Compatibility string `json:"compatibility,omitempty"`
	Deleted       bool   `json:"deleted"`
}

type RegistryOverview struct {
	Mode          string           `json:"mode"`
	Compatibility string           `json:"compatibility"`
	Types         []string         `json:"types"`
	Subjects      []SubjectSummary `json:"subjects"`
	// Total counts every subject matching the search and type filter.
	Total int `json:"total"`
	// Contexts lists the registry contexts that hold subjects ("." is the
	// default one), so the list can be narrowed to one.
	Contexts []string `json:"contexts"`
}

// RegistryQuery selects one page of subjects. Versions and compatibility
// are read for the returned page alone; a type filter needs every matching
// subject's latest schema first.
type RegistryQuery struct {
	ShowDeleted bool
	Search      string
	Type        string
	// Context keeps subjects of one registry context: "." for the default,
	// ".name" for a named one; empty keeps all.
	Context string
	Offset  int
	// Limit is the page size; 0 returns every matching subject.
	Limit int
}

func (c *Client) Registry(ctx context.Context, q RegistryQuery) (*RegistryOverview, error) {
	if err := c.requireRegistry(); err != nil {
		return nil, err
	}

	showDeleted := q.ShowDeleted
	base := ctx
	if showDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	out := &RegistryOverview{Subjects: []SubjectSummary{}, Types: []string{}}

	// Independent registry calls, one HTTP round trip each.
	var (
		names  []string
		err    error
		active = map[string]bool{}
		wg     sync.WaitGroup
	)

	wg.Go(func() {
		if g := c.registry.Compatibility(ctx); len(g) > 0 && g[0].Err == nil {
			out.Compatibility = g[0].Level.String()
		}
	})
	wg.Go(func() {
		if m := c.registry.Mode(ctx); len(m) > 0 && m[0].Err == nil {
			out.Mode = m[0].Mode.String()
		}
	})
	wg.Go(func() {
		if types, err := c.registry.SupportedTypes(ctx); err == nil {
			for _, t := range types {
				out.Types = append(out.Types, t.String())
			}
		} else {
			out.Types = slices.Clone(defaultSchemaTypes)
		}
	})
	// Subjects plus per-subject latest versions works on every registry;
	// GET /schemas is missing from some (e.g. Redpanda).
	wg.Go(func() { names, err = c.registry.Subjects(ctx) })

	// Contexts can exist before they hold subjects; registries without the
	// endpoint (e.g. Redpanda) still show contexts found in subject names.
	var extraContexts []string
	wg.Go(func() { extraContexts, _ = c.registry.Contexts(ctx) })

	if showDeleted {
		wg.Go(func() {
			if subjects, err := c.registry.Subjects(base); err == nil {
				for _, s := range subjects {
					active[s] = true
				}
			}
		})
	}

	wg.Wait()

	if err != nil {
		return nil, srErr("list subjects", err)
	}

	sort.Strings(names)

	out.Contexts = subjectContexts(names, extraContexts)

	if q.Context != "" {
		matched := names[:0]
		for _, n := range names {
			if subjectContext(n) == q.Context {
				matched = append(matched, n)
			}
		}
		names = matched
	}

	if search := strings.ToLower(strings.TrimSpace(q.Search)); search != "" {
		matched := names[:0]
		for _, n := range names {
			if strings.Contains(strings.ToLower(n), search) {
				matched = append(matched, n)
			}
		}
		names = matched
	}

	var bySubject map[string]*SubjectSummary
	if q.Type != "" {
		bySubject = c.subjectSummaries(ctx, names)
		matched := names[:0]
		for _, n := range names {
			if s := bySubject[n]; s != nil && s.Type == q.Type {
				matched = append(matched, n)
			}
		}
		names = matched
	}

	out.Total = len(names)
	names = pageOf(names, q.Offset, q.Limit)

	compat := map[string]string{}

	var compatWG sync.WaitGroup

	// Compatibility per subject runs alongside the summaries.
	compatWG.Go(func() {
		for _, r := range c.compatibilities(sr.WithParams(ctx, sr.DefaultToGlobal), names) {
			if r.Err == nil {
				compat[r.Subject] = r.Level.String()
			}
		}
	})

	if bySubject == nil {
		bySubject = c.subjectSummaries(ctx, names)
	}

	compatWG.Wait()

	for _, n := range names {
		s := bySubject[n]
		if s == nil {
			s = &SubjectSummary{Name: n}
		}

		s.Compatibility = compat[n]
		s.Deleted = showDeleted && !active[n]
		out.Subjects = append(out.Subjects, *s)
	}

	return out, nil
}

// subjectContext is the registry context of a subject: ".name" for a
// qualified ":.name:subject", "." otherwise.
func subjectContext(subject string) string {
	ctx, _ := splitSubjectContext(subject)

	return ctx
}

// splitSubjectContext splits a qualified ":.name:subject" into ".name" and
// subject; an unqualified subject is in the default context ".".
func splitSubjectContext(subject string) (string, string) {
	if rest, ok := strings.CutPrefix(subject, ":."); ok {
		if ctx, name, ok := strings.Cut(rest, ":"); ok {
			return "." + ctx, name
		}
	}

	return ".", subject
}

func subjectContexts(subjects, listed []string) []string {
	set := map[string]bool{".": true}
	for _, s := range subjects {
		set[subjectContext(s)] = true
	}

	for _, c := range listed {
		if c != "" {
			set[c] = true
		}
	}

	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}

	sort.Strings(out)

	return out
}

type SchemaVersion struct {
	Subject    string               `json:"subject"`
	Version    int                  `json:"version"`
	ID         int                  `json:"id"`
	Type       string               `json:"type"`
	Schema     string               `json:"schema"`
	References []sr.SchemaReference `json:"references"`
	Deleted    bool                 `json:"deleted"`
	// Messages lists protobuf message names, for producing.
	Messages []string `json:"messages,omitempty"`
}

type SubjectDetail struct {
	Name          string `json:"name"`
	Compatibility string `json:"compatibility"`
	// Override is true when the subject has its own compatibility level.
	Override   bool            `json:"override"`
	Mode       string          `json:"mode"`
	Versions   []SchemaVersion `json:"versions"`
	ReferredBy []string        `json:"referred_by"`
	// Topics are the topics this subject belongs to by naming: "<topic>-key"
	// and "<topic>-value" (TopicNameStrategy) and "<topic>-<record>"
	// (TopicRecordNameStrategy) for topics that exist.
	Topics []SubjectTopic `json:"topics"`
}

type SubjectTopic struct {
	Topic string `json:"topic"`
	// Role is key, value or record.
	Role string `json:"role"`
}

// defaultSchemaTypes are what a registry that cannot list its types is
// assumed to support.
var defaultSchemaTypes = []string{"AVRO", "PROTOBUF", "JSON"}

// registryParallel bounds concurrent registry requests so a large registry
// is not hit with thousands at once.
const registryParallel = 8

// allSchemasFrom is how many subjects make one GET /schemas cheaper than
// two requests per subject. Below it a page (50 subjects by default) does
// not download every schema text of the registry.
const allSchemasFrom = 200

// allSchemas reads every schema with one GET /schemas when that is cheaper
// than reading the subjects one by one; ok is false otherwise, and when the
// registry lacks the endpoint (e.g. Redpanda).
func (c *Client) allSchemas(ctx context.Context, subjects int) ([]sr.SubjectSchema, bool) {
	if subjects < allSchemasFrom {
		return nil, false
	}

	all, err := c.registry.AllSchemas(ctx)

	return all, err == nil && len(all) > 0
}

// subjectSummaries reads the subjects' latest version and version count.
// Many subjects (a type filter over all) are read with one GET /schemas;
// a page, and registries without that endpoint, use two requests per
// subject, in parallel.
func (c *Client) subjectSummaries(ctx context.Context, names []string) map[string]*SubjectSummary {
	out := make(map[string]*SubjectSummary, len(names))

	if all, ok := c.allSchemas(ctx, len(names)); ok {
		for _, s := range all {
			sum := out[s.Subject]
			if sum == nil {
				sum = &SubjectSummary{Name: s.Subject}
				out[s.Subject] = sum
			}

			sum.Versions++

			if s.Version > sum.LatestVersion {
				sum.LatestVersion, sum.LatestID, sum.Type = s.Version, s.ID, s.Type.String()
			}
		}

		complete := true

		for _, n := range names {
			if out[n] == nil {
				complete = false

				break
			}
		}

		if complete {
			return out
		}
	}

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)

	sem := make(chan struct{}, registryParallel)

	// Collected before any goroutine starts: they write out, and reading
	// it alongside them is a data race the runtime aborts on.
	var missing []string

	for _, n := range names {
		if out[n] == nil {
			missing = append(missing, n)
		}
	}

	for _, n := range missing {
		wg.Add(1)

		go func() {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			sum := &SubjectSummary{Name: n}

			if versions, err := c.registry.SubjectVersions(ctx, n); err == nil {
				sum.Versions = len(versions)
			}

			// The latest of a soft-deleted subject is only visible with deleted.
			if s, err := c.registry.SchemaByVersion(ctx, n, -1); err == nil {
				sum.LatestVersion, sum.LatestID, sum.Type = s.Version, s.ID, s.Type.String()
			}

			mu.Lock()
			out[n] = sum
			mu.Unlock()
		}()
	}

	wg.Wait()

	return out
}

// compatibilities reads per-subject compatibility in batches; the client
// otherwise starts one request per subject all at once.
func (c *Client) compatibilities(ctx context.Context, names []string) []sr.CompatibilityResult {
	out := make([]sr.CompatibilityResult, 0, len(names))

	for len(names) > 0 {
		n := min(len(names), registryParallel)
		out = append(out, c.registry.Compatibility(ctx, names[:n]...)...)
		names = names[n:]
	}

	return out
}

func (c *Client) Subject(ctx context.Context, subject string) (*SubjectDetail, error) {
	if err := c.requireRegistry(); err != nil {
		return nil, err
	}

	schemas, err := c.registry.Schemas(sr.WithParams(ctx, sr.ShowDeleted), subject)
	if err != nil {
		return nil, srErr("subject "+subject, err)
	}

	out := &SubjectDetail{Name: subject, Versions: []SchemaVersion{}, ReferredBy: []string{}, Topics: []SubjectTopic{}}

	// Independent lookups, each an HTTP round trip (or a Kafka one).
	var (
		live = map[int]bool{}
		wg   sync.WaitGroup
	)

	wg.Go(func() {
		if versions, err := c.registry.SubjectVersions(ctx, subject); err == nil {
			for _, v := range versions {
				live[v] = true
			}
		}
	})
	wg.Go(func() {
		if r := c.registry.Compatibility(ctx, subject); len(r) > 0 && r[0].Err == nil {
			out.Compatibility = r[0].Level.String()
			out.Override = true
		} else if g := c.registry.Compatibility(ctx); len(g) > 0 && g[0].Err == nil {
			out.Compatibility = g[0].Level.String()
		}
	})
	wg.Go(func() {
		if m := c.registry.Mode(sr.WithParams(ctx, sr.DefaultToGlobal), subject); len(m) > 0 && m[0].Err == nil {
			out.Mode = m[0].Mode.String()
		}
	})
	wg.Go(func() { out.Topics = c.subjectTopics(ctx, subject) })
	wg.Wait()

	for _, s := range schemas {
		v := SchemaVersion{
			Subject: s.Subject, Version: s.Version, ID: s.ID, Type: s.Type.String(),
			Schema: s.Schema.Schema, References: s.References, Deleted: !live[s.Version],
		}

		if v.References == nil {
			v.References = []sr.SchemaReference{}
		}

		if s.Type == sr.TypeProtobuf && c.codecs != nil {
			if cd, err := c.codecs.get(ctx, s.ID); err == nil {
				v.Messages = protoMessageNames(cd.proto)
			}
		}

		out.Versions = append(out.Versions, v)
	}

	sort.Slice(out.Versions, func(i, j int) bool { return out.Versions[i].Version > out.Versions[j].Version })

	if len(out.Versions) > 0 {
		latest := out.Versions[0]
		if refs, err := c.registry.SchemaReferences(ctx, subject, latest.Version); err == nil {
			for _, r := range refs {
				out.ReferredBy = append(out.ReferredBy, fmt.Sprintf("%s v%d", r.Subject, r.Version))
			}
		}
	}

	return out, nil
}

// subjectTopics finds the existing topics a subject is named after. Only
// naming is known: a producer may use any subject with any topic, so this
// is what the default strategies imply, not a record of actual use.
func (c *Client) subjectTopics(ctx context.Context, subject string) []SubjectTopic {
	out := []SubjectTopic{}

	// Strip a registry context prefix (":.ctx:subject") first.
	_, name := splitSubjectContext(subject)

	candidates := map[string]string{}
	for _, role := range []string{"key", "value"} {
		if t, ok := strings.CutSuffix(name, "-"+role); ok && t != "" {
			candidates[t] = role
		}
	}

	// TopicRecordNameStrategy: "<topic>-<fully.qualified.Record>". Topic
	// names may hold dashes too, so every split point is a candidate.
	for i := range len(name) {
		if name[i] == '-' && i > 0 && i < len(name)-1 {
			if _, ok := candidates[name[:i]]; !ok {
				candidates[name[:i]] = "record"
			}
		}
	}

	if len(candidates) == 0 {
		return out
	}

	names := make([]string, 0, len(candidates))
	for t := range candidates {
		names = append(names, t)
	}

	details, _, err := c.listTopics(ctx, names...)
	if err != nil {
		return out
	}

	for t, role := range candidates {
		if d, ok := details[t]; ok && d.Err == nil {
			out = append(out, SubjectTopic{Topic: t, Role: role})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })

	return out
}

type CreateSchemaRequest struct {
	Subject    string               `json:"subject"`
	Type       string               `json:"type"`
	Schema     string               `json:"schema"`
	References []sr.SchemaReference `json:"references"`
	Normalize  bool                 `json:"normalize"`
}

func (r CreateSchemaRequest) schema() (sr.Schema, error) {
	var t sr.SchemaType
	if err := t.UnmarshalText([]byte(r.Type)); err != nil {
		return sr.Schema{}, ErrInvalid{err}
	}

	if strings.TrimSpace(r.Schema) == "" {
		return sr.Schema{}, ErrInvalid{errors.New("schema is required")}
	}

	return sr.Schema{Schema: r.Schema, Type: t, References: r.References}, nil
}

func (c *Client) CreateSchema(ctx context.Context, req CreateSchemaRequest) (*SchemaVersion, error) {
	if err := c.requireRegistryWrite(); err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.Subject) == "" {
		return nil, ErrInvalid{errors.New("subject is required")}
	}

	s, err := req.schema()
	if err != nil {
		return nil, err
	}

	if req.Normalize {
		ctx = sr.WithParams(ctx, sr.Normalize)
	}

	ss, err := c.registry.CreateSchema(ctx, req.Subject, s)
	if err != nil {
		return nil, srErr("create schema", err)
	}

	return &SchemaVersion{Subject: ss.Subject, Version: ss.Version, ID: ss.ID, Type: ss.Type.String()}, nil
}

type CompatibilityCheck struct {
	Compatible bool     `json:"compatible"`
	Messages   []string `json:"messages"`
}

func (c *Client) CheckCompatibility(ctx context.Context, req CreateSchemaRequest) (*CompatibilityCheck, error) {
	if err := c.requireRegistry(); err != nil {
		return nil, err
	}

	s, err := req.schema()
	if err != nil {
		return nil, err
	}

	res, err := c.registry.CheckCompatibility(sr.WithParams(ctx, sr.Verbose), req.Subject, -1, s)
	if err != nil {
		// A new subject has nothing to be incompatible with.
		var re *sr.ResponseError
		if errors.As(err, &re) && re.StatusCode == http.StatusNotFound {
			return &CompatibilityCheck{Compatible: true, Messages: []string{}}, nil
		}

		return nil, srErr("check compatibility", err)
	}

	msgs := res.Messages
	if msgs == nil {
		msgs = []string{}
	}

	return &CompatibilityCheck{Compatible: res.Is, Messages: msgs}, nil
}

func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	if err := c.requireRegistryWrite(); err != nil {
		return err
	}

	var res []sr.CompatibilityResult

	subjects := []string{}
	if subject != "" {
		subjects = append(subjects, subject)
	}

	if level == "" || strings.EqualFold(level, "DEFAULT") {
		if subject == "" {
			return ErrInvalid{errors.New("global compatibility cannot be reset")}
		}

		res = c.registry.ResetCompatibility(ctx, subjects...)
	} else {
		var l sr.CompatibilityLevel
		if err := l.UnmarshalText([]byte(level)); err != nil {
			return ErrInvalid{err}
		}

		res = c.registry.SetCompatibility(ctx, sr.SetCompatibility{Level: l}, subjects...)
	}

	for _, r := range res {
		if r.Err != nil {
			return srErr("set compatibility", r.Err)
		}
	}

	return nil
}

// DeleteSubject soft deletes, or hard deletes when permanent. A hard delete
// requires a prior soft delete, so both steps run.
func (c *Client) DeleteSubject(ctx context.Context, subject string, permanent bool) error {
	if err := c.requireRegistryWrite(); err != nil {
		return err
	}

	return softThenHard("delete subject", permanent, func(how sr.DeleteHow) error {
		_, err := c.registry.DeleteSubject(ctx, subject, how)

		return err
	})
}

// softThenHard runs a registry delete soft, then hard when permanent: a hard
// delete requires a prior soft one. A failed soft delete of something already
// soft-deleted does not stop the hard one.
func softThenHard(op string, permanent bool, del func(sr.DeleteHow) error) error {
	if err := del(sr.SoftDelete); err != nil && !permanent {
		return srErr(op, err)
	}

	if permanent {
		if err := del(sr.HardDelete); err != nil {
			return srErr(op, err)
		}
	}

	return nil
}

func (c *Client) DeleteSchemaVersion(ctx context.Context, subject string, version int, permanent bool) error {
	if err := c.requireRegistryWrite(); err != nil {
		return err
	}

	return softThenHard("delete schema", permanent, func(how sr.DeleteHow) error {
		return c.registry.DeleteSchema(ctx, subject, version, how)
	})
}
