package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ErrUnsupported means the broker does not implement the request, for
// example ACLs on a cluster without an authorizer.
var ErrUnsupported = errors.New("not supported by this cluster")

func unsupported(err error) error {
	if errors.Is(err, kerr.SecurityDisabled) || errors.Is(err, kerr.UnsupportedVersion) ||
		errors.Is(err, kerr.UnsupportedSaslMechanism) {
		return fmt.Errorf("%w: %w", ErrUnsupported, err)
	}

	return err
}

// ─── ACLs ─────────────────────────────────────────────────

type ACL struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	PatternType  string `json:"pattern_type"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
}

func (c *Client) ACLs(ctx context.Context) ([]ACL, error) {
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("describe acls; %w", unsupported(err))
	}

	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		msg := ""
		if resp.ErrorMessage != nil {
			msg = " " + *resp.ErrorMessage
		}

		return nil, fmt.Errorf("describe acls; %w%s", unsupported(err), msg)
	}

	out := []ACL{}

	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			out = append(out, ACL{
				Principal:    a.Principal,
				Host:         a.Host,
				ResourceType: r.ResourceType.String(),
				ResourceName: r.ResourceName,
				PatternType:  r.ResourcePatternType.String(),
				Operation:    a.Operation.String(),
				Permission:   a.PermissionType.String(),
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Principal != b.Principal {
			return a.Principal < b.Principal
		}

		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}

		if a.ResourceName != b.ResourceName {
			return a.ResourceName < b.ResourceName
		}

		return a.Operation < b.Operation
	})

	return out, nil
}

type aclEnums struct {
	resource   kmsg.ACLResourceType
	pattern    kmsg.ACLResourcePatternType
	operation  kmsg.ACLOperation
	permission kmsg.ACLPermissionType
}

func parseACL(a ACL) (aclEnums, error) {
	var (
		e   aclEnums
		err error
	)

	if e.resource, err = kmsg.ParseACLResourceType(a.ResourceType); err != nil {
		return e, ErrInvalid{fmt.Errorf("resource type; %w", err)}
	}

	pattern := a.PatternType
	if pattern == "" {
		pattern = "LITERAL"
	}

	if e.pattern, err = kmsg.ParseACLResourcePatternType(pattern); err != nil {
		return e, ErrInvalid{fmt.Errorf("pattern type; %w", err)}
	}

	if e.operation, err = kmsg.ParseACLOperation(a.Operation); err != nil {
		return e, ErrInvalid{fmt.Errorf("operation; %w", err)}
	}

	if e.permission, err = kmsg.ParseACLPermissionType(a.Permission); err != nil {
		return e, ErrInvalid{fmt.Errorf("permission; %w", err)}
	}

	return e, nil
}

func normalizeACL(a *ACL) error {
	a.Principal = strings.TrimSpace(a.Principal)
	if a.Principal == "" {
		return ErrInvalid{errors.New("principal is required, e.g. User:alice")}
	}

	if !strings.Contains(a.Principal, ":") {
		a.Principal = "User:" + a.Principal
	}

	if a.Host == "" {
		a.Host = "*"
	}

	if strings.EqualFold(a.ResourceType, "CLUSTER") {
		a.ResourceName = "kafka-cluster"
	}

	if a.ResourceName == "" {
		return ErrInvalid{errors.New("resource name is required")}
	}

	return nil
}

// CreateACLs creates every ACL; the first failure is returned.
func (c *Client) CreateACLs(ctx context.Context, acls []ACL) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	req := kmsg.NewPtrCreateACLsRequest()

	for _, a := range acls {
		if err := normalizeACL(&a); err != nil {
			return err
		}

		e, err := parseACL(a)
		if err != nil {
			return err
		}

		cr := kmsg.NewCreateACLsRequestCreation()
		cr.Principal = a.Principal
		cr.Host = a.Host
		cr.ResourceType = e.resource
		cr.ResourceName = a.ResourceName
		cr.ResourcePatternType = e.pattern
		cr.Operation = e.operation
		cr.PermissionType = e.permission
		req.Creations = append(req.Creations, cr)
	}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("create acls; %w", unsupported(err))
	}

	for _, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			msg := ""
			if r.ErrorMessage != nil {
				msg = " " + *r.ErrorMessage
			}

			return fmt.Errorf("create acl; %w%s", unsupported(err), msg)
		}
	}

	return nil
}

// DeleteACL removes exactly the matching ACL.
func (c *Client) DeleteACL(ctx context.Context, a ACL) (int, error) {
	if err := c.requireWritable(); err != nil {
		return 0, err
	}

	e, err := parseACL(a)
	if err != nil {
		return 0, err
	}

	f := kmsg.NewDeleteACLsRequestFilter()
	f.Principal = &a.Principal
	f.Host = &a.Host
	f.ResourceType = e.resource
	f.ResourceName = &a.ResourceName
	f.ResourcePatternType = e.pattern
	f.Operation = e.operation
	f.PermissionType = e.permission

	req := kmsg.NewPtrDeleteACLsRequest()
	req.Filters = append(req.Filters, f)

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return 0, fmt.Errorf("delete acl; %w", unsupported(err))
	}

	deleted := 0

	for _, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return 0, fmt.Errorf("delete acl; %w", unsupported(err))
		}

		deleted += len(r.MatchingACLs)
	}

	return deleted, nil
}

// ─── SCRAM users ──────────────────────────────────────────

type User struct {
	Name       string   `json:"name"`
	Mechanisms []string `json:"mechanisms"`
}

func (c *Client) Users(ctx context.Context) ([]User, error) {
	described, err := c.admin.DescribeUserSCRAMs(ctx)
	if err != nil {
		return nil, fmt.Errorf("describe users; %w", unsupported(err))
	}

	out := []User{}

	for _, d := range described.Sorted() {
		if d.Err != nil {
			continue
		}

		u := User{Name: d.User, Mechanisms: []string{}}
		for _, ci := range d.CredInfos {
			u.Mechanisms = append(u.Mechanisms, ci.Mechanism.String())
		}

		out = append(out, u)
	}

	return out, nil
}

type UpsertUserRequest struct {
	Name       string `json:"name"`
	Password   string `json:"password"`
	Mechanism  string `json:"mechanism"`
	Iterations int32  `json:"iterations"`
}

func scramMechanism(s string) (kadm.ScramMechanism, error) {
	switch strings.ToUpper(strings.ReplaceAll(s, "_", "-")) {
	case "", "SCRAM-SHA-256":
		return kadm.ScramSha256, nil
	case "SCRAM-SHA-512":
		return kadm.ScramSha512, nil
	}

	return 0, ErrInvalid{fmt.Errorf("unknown mechanism %q", s)}
}

func (c *Client) UpsertUser(ctx context.Context, req UpsertUserRequest) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	if strings.TrimSpace(req.Name) == "" || req.Password == "" {
		return ErrInvalid{errors.New("name and password are required")}
	}

	mech, err := scramMechanism(req.Mechanism)
	if err != nil {
		return err
	}

	resp, err := c.admin.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User: req.Name, Mechanism: mech, Iterations: req.Iterations, Password: req.Password,
	}})
	if err != nil {
		return fmt.Errorf("upsert user; %w", unsupported(err))
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("upsert user; %w %s", unsupported(r.Err), r.ErrMessage)
		}
	}

	return nil
}

func (c *Client) DeleteUser(ctx context.Context, name, mechanism string) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	var mechs []kadm.ScramMechanism

	if mechanism == "" {
		described, err := c.admin.DescribeUserSCRAMs(ctx, name)
		if err != nil {
			return fmt.Errorf("describe user; %w", unsupported(err))
		}

		for _, ci := range described[name].CredInfos {
			mechs = append(mechs, ci.Mechanism)
		}

		if len(mechs) == 0 {
			return fmt.Errorf("user %q; %w", name, ErrNotFound)
		}
	} else {
		m, err := scramMechanism(mechanism)
		if err != nil {
			return err
		}

		mechs = []kadm.ScramMechanism{m}
	}

	del := make([]kadm.DeleteSCRAM, 0, len(mechs))
	for _, m := range mechs {
		del = append(del, kadm.DeleteSCRAM{User: name, Mechanism: m})
	}

	resp, err := c.admin.AlterUserSCRAMs(ctx, del, nil)
	if err != nil {
		return fmt.Errorf("delete user; %w", unsupported(err))
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("delete user; %w %s", unsupported(r.Err), r.ErrMessage)
		}
	}

	return nil
}

// ─── Quotas ───────────────────────────────────────────────

type QuotaEntity struct {
	Type string `json:"type"`
	// Name nil means the default entity for Type.
	Name *string `json:"name"`
}

type Quota struct {
	Entity []QuotaEntity      `json:"entity"`
	Values map[string]float64 `json:"values"`
}

func (c *Client) Quotas(ctx context.Context) ([]Quota, error) {
	described, err := c.admin.DescribeClientQuotas(ctx, false, nil)
	if err != nil {
		return nil, fmt.Errorf("describe quotas; %w", unsupported(err))
	}

	out := []Quota{}

	for _, d := range described {
		q := Quota{Values: map[string]float64{}}
		for _, e := range d.Entity {
			q.Entity = append(q.Entity, QuotaEntity{Type: e.Type, Name: e.Name})
		}

		for _, v := range d.Values {
			q.Values[v.Key] = v.Value
		}

		out = append(out, q)
	}

	return out, nil
}

type AlterQuotaRequest struct {
	Entity []QuotaEntity `json:"entity"`
	// Set maps keys to values; Remove lists keys to drop.
	Set    map[string]float64 `json:"set"`
	Remove []string           `json:"remove"`
}

func (c *Client) AlterQuota(ctx context.Context, req AlterQuotaRequest) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	if len(req.Entity) == 0 {
		return ErrInvalid{errors.New("entity is required")}
	}

	entry := kadm.AlterClientQuotaEntry{}
	for _, e := range req.Entity {
		entry.Entity = append(entry.Entity, kadm.ClientQuotaEntityComponent{Type: e.Type, Name: e.Name})
	}

	for k, v := range req.Set {
		entry.Ops = append(entry.Ops, kadm.AlterClientQuotaOp{Key: k, Value: v})
	}

	for _, k := range req.Remove {
		entry.Ops = append(entry.Ops, kadm.AlterClientQuotaOp{Key: k, Remove: true})
	}

	resp, err := c.admin.AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{entry})
	if err != nil {
		return fmt.Errorf("alter quota; %w", unsupported(err))
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("alter quota; %w %s", unsupported(r.Err), r.ErrMessage)
		}
	}

	return nil
}
