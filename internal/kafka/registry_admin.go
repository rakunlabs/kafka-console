package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

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
}

// RegistryQuery selects one page of subjects. Versions and compatibility
// are read for the returned page alone; a type filter needs every matching
// subject's latest schema first.
type RegistryQuery struct {
	ShowDeleted bool
	Search      string
	Type        string
	Offset      int
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

	if g := c.registry.Compatibility(ctx); len(g) > 0 && g[0].Err == nil {
		out.Compatibility = g[0].Level.String()
	}

	if m := c.registry.Mode(ctx); len(m) > 0 && m[0].Err == nil {
		out.Mode = m[0].Mode.String()
	}

	if types, err := c.registry.SupportedTypes(ctx); err == nil {
		for _, t := range types {
			out.Types = append(out.Types, t.String())
		}
	} else {
		out.Types = []string{"AVRO", "PROTOBUF", "JSON"}
	}

	// Subjects plus per-subject latest versions works on every registry;
	// GET /schemas is missing from some (e.g. Redpanda).
	names, err := c.registry.Subjects(ctx)
	if err != nil {
		return nil, srErr("list subjects", err)
	}

	sort.Strings(names)

	if search := strings.ToLower(strings.TrimSpace(q.Search)); search != "" {
		matched := names[:0]
		for _, n := range names {
			if strings.Contains(strings.ToLower(n), search) {
				matched = append(matched, n)
			}
		}
		names = matched
	}

	active := map[string]bool{}
	if showDeleted {
		if subjects, err := c.registry.Subjects(base); err == nil {
			for _, s := range subjects {
				active[s] = true
			}
		}
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

	if bySubject == nil {
		bySubject = c.subjectSummaries(ctx, names)
	}

	compat := map[string]string{}
	for _, r := range c.compatibilities(sr.WithParams(ctx, sr.DefaultToGlobal), names) {
		if r.Err == nil {
			compat[r.Subject] = r.Level.String()
		}
	}

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
}

// registryParallel bounds concurrent registry requests so a large registry
// is not hit with thousands at once.
const registryParallel = 8

// subjectSummaries reads every subject's latest version and version count.
// One GET /schemas covers the whole registry; registries without it (e.g.
// Redpanda) fall back to two requests per subject.
func (c *Client) subjectSummaries(ctx context.Context, names []string) map[string]*SubjectSummary {
	out := make(map[string]*SubjectSummary, len(names))

	if all, err := c.registry.AllSchemas(ctx); err == nil && (len(all) > 0 || len(names) == 0) {
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

	for _, n := range names {
		if out[n] != nil {
			continue
		}

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

	live := map[int]bool{}
	if versions, err := c.registry.SubjectVersions(ctx, subject); err == nil {
		for _, v := range versions {
			live[v] = true
		}
	}

	out := &SubjectDetail{Name: subject, Versions: []SchemaVersion{}, ReferredBy: []string{}}

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

	if r := c.registry.Compatibility(ctx, subject); len(r) > 0 && r[0].Err == nil {
		out.Compatibility = r[0].Level.String()
		out.Override = true
	} else if g := c.registry.Compatibility(ctx); len(g) > 0 && g[0].Err == nil {
		out.Compatibility = g[0].Level.String()
	}

	if m := c.registry.Mode(sr.WithParams(ctx, sr.DefaultToGlobal), subject); len(m) > 0 && m[0].Err == nil {
		out.Mode = m[0].Mode.String()
	}

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
	if err := c.requireRegistry(); err != nil {
		return nil, err
	}

	if err := c.requireWritable(); err != nil {
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
	if err := c.requireRegistry(); err != nil {
		return err
	}

	if err := c.requireWritable(); err != nil {
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
	if err := c.requireRegistry(); err != nil {
		return err
	}

	if err := c.requireWritable(); err != nil {
		return err
	}

	if _, err := c.registry.DeleteSubject(ctx, subject, sr.SoftDelete); err != nil && !permanent {
		return srErr("delete subject", err)
	}

	if permanent {
		if _, err := c.registry.DeleteSubject(ctx, subject, sr.HardDelete); err != nil {
			return srErr("delete subject", err)
		}
	}

	return nil
}

func (c *Client) DeleteSchemaVersion(ctx context.Context, subject string, version int, permanent bool) error {
	if err := c.requireRegistry(); err != nil {
		return err
	}

	if err := c.requireWritable(); err != nil {
		return err
	}

	if err := c.registry.DeleteSchema(ctx, subject, version, sr.SoftDelete); err != nil && !permanent {
		return srErr("delete schema", err)
	}

	if permanent {
		if err := c.registry.DeleteSchema(ctx, subject, version, sr.HardDelete); err != nil {
			return srErr("delete schema", err)
		}
	}

	return nil
}
