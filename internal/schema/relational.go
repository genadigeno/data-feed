package schema

import (
	"math/big"
	"strconv"
	"strings"
)

// Relational problem codes.
const (
	CodeNoTables       = "ERR_NO_TABLES"
	CodeTableName      = "ERR_INVALID_TABLE_NAME"
	CodeCountRequired  = "ERR_COUNT_REQUIRED"
	CodeCountForbidden = "ERR_COUNT_NOT_ALLOWED"
	CodePrimaryKey     = "ERR_INVALID_PRIMARY_KEY"
	CodeFKTarget       = "ERR_FK_TARGET_MISSING"
	CodeFKTargetKey    = "ERR_FK_TARGET_NOT_KEY"
	CodeOwner          = "ERR_INVALID_OWNER"
	CodeOwnershipCycle = "ERR_OWNERSHIP_CYCLE"
	CodeFKMayBeEmpty   = "ERR_FK_MAY_BE_EMPTY"
	CodeSelect         = "ERR_INVALID_SELECT"
	CodeDerive         = "ERR_INVALID_DERIVE"
	CodeUniqueKey      = "ERR_INVALID_UNIQUE_KEY"
	CodeIndexOverflow  = "ERR_INDEX_OVERFLOW"
)

// GeneratorOrdinal names the generator that emits a child's 1-based ordinal within
// its parent. With the owner key it forms a composite that is unique by construction.
const GeneratorOrdinal = "ordinal"

// maxIndexSpace is the largest row index space: record indices are 63-bit safe.
var maxIndexSpace = new(big.Int).Lsh(big.NewInt(1), 63)

func (v *validator) relational(r *RelationalSpec) {
	if len(r.Tables) == 0 {
		v.add("tables", CodeNoTables, "a relational schema needs at least one table")
		return
	}
	if len(r.Tables) > MaxTables {
		v.add("tables", CodeLimit, "%d tables exceed the limit of %d", len(r.Tables), MaxTables)
		return
	}
	for i := range r.Tables {
		v.tableShape(&r.Tables[i], r)
	}
	if v.oversizedTable(r) {
		return // the limit is reported; cross-table checks search fields by name
	}
	acyclic := v.ownershipGraph(r)
	for i := range r.Tables {
		v.tableReferences(&r.Tables[i], r)
	}

	if acyclic {
		spaces := v.indexSpaces(r)
		for i := range r.Tables {
			t := &r.Tables[i]
			if space := spaces[t.Name]; space != nil {
				v.tableUnique(t, r, space)
				v.distinctSelects(t, r, spaces)
			}
		}
	}

	if v.defCycle {
		return
	}
	for i := range r.Tables {
		t := &r.Tables[i]
		path := join("tables", t.Name)
		if !v.opts.Output.Nested {
			v.csvColumns(join(path, "fields"), t.Fields)
			v.recordSize(join(path, "fields"), fieldsMaxBytes(t.Fields, v.defs))
		}
	}
	if v.opts.Output.Nested && acyclic {
		v.nestedLayout(r)
	}
}

func (v *validator) oversizedTable(r *RelationalSpec) bool {
	limit := v.opts.Limits.MaxFields
	if limit <= 0 {
		return false
	}
	for i := range r.Tables {
		if int64(len(r.Tables[i].Fields)) > limit {
			return true
		}
	}
	return false
}

func (v *validator) recordSize(path string, size int64) {
	if limit := v.opts.Limits.MaxRecordBytes; limit > 0 && size > limit {
		v.add(path, CodeRecordTooLarge, "records may reach %d bytes, above the limit of %d", size, limit)
	}
}

func (v *validator) tableShape(t *Table, r *RelationalSpec) {
	path := join("tables", t.Name)
	if !tableNamePattern.MatchString(t.Name) {
		v.add(path, CodeTableName, "table names must match %s, because they name output directories", tableNamePattern)
	}
	if len(t.Fields) == 0 {
		v.add(join(path, "fields"), CodeRequiredFields, "a table needs at least one field")
	}
	v.fields(join(path, "fields"), t.Fields, 1)

	switch {
	case t.Owner == nil && t.Count == nil:
		v.add(join(path, "count"), CodeCountRequired, "a table without an owner needs a count")
	case t.Owner != nil && t.Count != nil:
		v.add(join(path, "count"), CodeCountForbidden, "an owned table's row count comes from per_parent")
	case t.Count != nil && *t.Count <= 0:
		v.add(join(path, "count"), CodeRange, "must be greater than zero")
	}

	if t.PrimaryKey != "" {
		v.primaryKey(t, path)
	}

	if o := t.Owner; o != nil {
		op := join(path, "owner")
		if r.Table(o.Table) == nil {
			v.add(join(op, "ref"), CodeOwner, "no table named %q", o.Table)
		}
		pp := join(op, "per_parent")
		if o.PerParent.Min < 0 || o.PerParent.Max < 1 || o.PerParent.Min > o.PerParent.Max {
			v.add(pp, CodeRange, "need 0 ≤ min ≤ max and max ≥ 1")
		}
		if d := o.PerParent.Distribution; d != nil {
			v.distribution(join(pp, "distribution"), d, DistUniform, DistNormal, DistZipf)
		}
	}
}

func (v *validator) primaryKey(t *Table, path string) {
	p := join(path, "primary_key")
	pk := t.Field(t.PrimaryKey)
	switch {
	case pk == nil:
		v.add(p, CodePrimaryKey, "no field named %q", t.PrimaryKey)
	case !pk.Type.isScalar() || pk.Type == TypeNull || pk.Type == TypeBoolean || pk.Type == TypeFloat:
		v.add(p, CodePrimaryKey, "type %s cannot be a primary key", pk.Type)
	case pk.Type == TypeReference:
		v.add(p, CodePrimaryKey, "a foreign key cannot be the primary key: give the table its own key")
	case pk.Nullable:
		v.add(p, CodePrimaryKey, "a primary key cannot be nullable")
	}
}

// ownershipGraph checks that owning relationships form a tree of tables. Selecting
// references may cycle freely, because keys are computed from indices, but a table
// whose rows only exist per parent cannot be its own ancestor.
func (v *validator) ownershipGraph(r *RelationalSpec) bool {
	const (
		unvisited = iota
		visiting
		done
	)
	state := map[string]int{}
	ok := true
	var visit func(t *Table, trail []string)
	visit = func(t *Table, trail []string) {
		state[t.Name] = visiting
		trail = append(trail, t.Name)
		if t.Owner != nil {
			if parent := r.Table(t.Owner.Table); parent != nil {
				switch state[parent.Name] {
				case visiting:
					ok = false
					v.add(join(join("tables", t.Name), "owner.ref"), CodeOwnershipCycle,
						"ownership forms a cycle: %s", strings.Join(append(trail, parent.Name), " -> "))
				case unvisited:
					visit(parent, trail)
				}
			} else {
				ok = false
			}
		}
		state[t.Name] = done
	}
	for i := range r.Tables {
		if state[r.Tables[i].Name] == unvisited {
			visit(&r.Tables[i], nil)
		}
	}
	return ok
}

func (v *validator) tableReferences(t *Table, r *RelationalSpec) {
	path := join("tables", t.Name)
	var owners []*FieldDef
	for i := range t.Fields {
		f := &t.Fields[i]
		p := join(join(path, "fields"), f.Name)
		if f.Ref != nil {
			if f.Ref.Owner {
				owners = append(owners, f)
			}
			v.reference(t, f, p, r)
		}
		if f.Derive != nil {
			v.derive(t, f, p, r)
		}
	}

	switch {
	case t.Owner != nil && len(owners) == 0:
		v.add(join(path, "owner"), CodeOwner, "needs a field with owner: true referring to %s", t.Owner.Table)
	case t.Owner == nil && len(owners) > 0:
		v.add(join(path, "owner"), CodeOwner, "a field has owner: true but the table declares no owner")
	case len(owners) > 1:
		v.add(join(path, "owner"), CodeOwner, "%d fields have owner: true; only one can", len(owners))
	case t.Owner != nil && owners[0].Ref.Table != t.Owner.Table:
		v.add(join(path, "owner"), CodeOwner, "field %s refers to %s but the owner is %s", owners[0].Name, owners[0].Ref.Table, t.Owner.Table)
	}
}

func (v *validator) reference(t *Table, f *FieldDef, path string, r *RelationalSpec) {
	ref := f.Ref
	rp := join(path, "ref")
	target := r.Table(ref.Table)
	if target == nil {
		v.add(rp, CodeFKTarget, "no table named %q", ref.Table)
		return
	}
	tf := target.Field(ref.Field)
	if tf == nil {
		v.add(rp, CodeFKTarget, "table %s has no field %q", ref.Table, ref.Field)
		return
	}
	switch {
	case ref.Field != target.PrimaryKey && !tf.Unique:
		v.add(rp, CodeFKTargetKey, "%s.%s must be the primary key or unique", ref.Table, ref.Field)
	case tf.Nullable:
		v.add(rp, CodeFKTargetKey, "%s.%s is nullable, so a reference to it could find nothing", ref.Table, ref.Field)
	case tf.Type == TypeReference:
		v.add(rp, CodeFKTargetKey, "%s.%s is itself a reference", ref.Table, ref.Field)
	}

	if ref.Owner {
		if f.Nullable {
			v.add(path, CodeOwner, "an owning reference cannot be nullable: every row has a parent")
		}
		if ref.Select != nil {
			v.add(join(path, "select"), CodeSelect, "an owning reference has its parent chosen by per_parent")
		}
		return
	}

	if s := ref.Select; s != nil {
		sp := join(path, "select")
		if s.Distribution != nil {
			v.distribution(join(sp, "distribution"), s.Distribution, DistUniform, DistNormal, DistZipf)
		}
		switch s.Mode {
		case SelectAny:
		case SelectEarlier:
			if ref.Table != t.Name {
				v.add(join(sp, "mode"), CodeSelect, "earlier applies to self references only")
			} else if !f.Nullable {
				v.add(join(sp, "mode"), CodeSelect, "earlier needs nullable: true, because the first rows have no earlier row")
			}
		default:
			v.add(join(sp, "mode"), CodeSelect, "%q is not a selection mode", s.Mode)
		}
		if s.Distinct && t.Owner == nil {
			v.add(join(sp, "distinct"), CodeSelect, "distinct means no repeats per owner, so the table needs an owner")
		}
	}

	if !f.Nullable && mayBeEmpty(target, r) {
		v.add(rp, CodeFKMayBeEmpty, "%s may have no rows because per_parent allows 0: make this field nullable or set per_parent.min to at least 1", ref.Table)
	}
}

// mayBeEmpty reports whether a table can end up with no rows at all.
func mayBeEmpty(t *Table, r *RelationalSpec) bool {
	for hops := 0; t != nil && hops <= len(r.Tables); hops++ {
		if t.Owner == nil {
			return false
		}
		if t.Owner.PerParent.Min == 0 {
			return true
		}
		t = r.Table(t.Owner.Table)
	}
	return false
}

func (v *validator) derive(t *Table, f *FieldDef, path string, r *RelationalSpec) {
	p := join(path, "derive")
	d := f.Derive
	if d.From != "owner" {
		v.add(join(p, "from"), CodeDerive, "%q: only owner is supported", d.From)
		return
	}
	if t.Owner == nil {
		v.add(p, CodeDerive, "the table has no owner to derive from")
		return
	}
	parent := r.Table(t.Owner.Table)
	if parent == nil {
		return
	}
	source := parent.Field(d.Field)
	if source == nil {
		v.add(join(p, "field"), CodeDerive, "table %s has no field %q", parent.Name, d.Field)
		return
	}
	if source.Type != f.Type {
		v.add(join(p, "field"), CodeDerive, "%s.%s is %s, this field is %s", parent.Name, d.Field, source.Type, f.Type)
	}
	if source.Type == TypeReference || !source.Type.isScalar() {
		v.add(join(p, "field"), CodeDerive, "cannot derive from type %s", source.Type)
	}
	if d.OffsetMin != nil || d.OffsetMax != nil {
		if f.Type.class() != classTemporal {
			v.add(join(p, "offset"), CodeDerive, "offset applies to date and time types only")
		}
		if d.OffsetMin != nil && d.OffsetMax != nil && *d.OffsetMin > *d.OffsetMax {
			v.add(join(p, "offset"), CodeRange, "min is above max")
		}
	}
}

// indexSpaces computes each table's row index space: its count, or for an owned table
// the parent's space times per_parent.max. Sparse, but stable, and what uniqueness
// encoders and row seeds run on.
func (v *validator) indexSpaces(r *RelationalSpec) map[string]*big.Int {
	spaces := map[string]*big.Int{}
	var space func(t *Table) *big.Int
	space = func(t *Table) *big.Int {
		if s, ok := spaces[t.Name]; ok {
			return s
		}
		var out *big.Int
		switch {
		case t.Owner == nil:
			if t.Count != nil && *t.Count > 0 {
				out = big.NewInt(*t.Count)
			}
		default:
			parent := r.Table(t.Owner.Table)
			if parent == nil {
				break
			}
			if p := space(parent); p != nil && t.Owner.PerParent.Max > 0 {
				out = new(big.Int).Mul(p, big.NewInt(t.Owner.PerParent.Max))
			}
		}
		if out != nil && out.Cmp(maxIndexSpace) > 0 {
			v.add(join("tables", t.Name), CodeIndexOverflow, "the row index space %s exceeds 2^63: reduce count or per_parent.max", out)
			out = nil
		}
		spaces[t.Name] = out
		return out
	}
	for i := range r.Tables {
		space(&r.Tables[i])
	}
	return spaces
}

func (v *validator) tableUnique(t *Table, r *RelationalSpec, space *big.Int) {
	path := join("tables", t.Name)
	for i := range t.Fields {
		f := &t.Fields[i]
		isKey := f.Unique || f.Name == t.PrimaryKey
		if isKey && f.Type.isScalar() && f.Type != TypeReference {
			v.uniqueField(join(join(path, "fields"), f.Name), f, space)
		}
	}
	for i, k := range t.UniqueKeys {
		v.uniqueKey(path+".unique_keys["+strconv.Itoa(i)+"]", t, k, space)
	}
}

func (v *validator) uniqueKey(path string, t *Table, k UniqueKey, space *big.Int) {
	if len(k.Fields) < 2 {
		v.add(join(path, "fields"), CodeUniqueKey, "a composite key needs at least two fields; use unique: true for one")
		return
	}
	seen := map[string]bool{}
	var hasOwnerKey, hasOrdinal bool
	for _, name := range k.Fields {
		f := t.Field(name)
		if f == nil {
			v.add(join(path, "fields"), CodeUniqueKey, "no field named %q", name)
			return
		}
		if seen[name] {
			v.add(join(path, "fields"), CodeUniqueKey, "%q is listed twice", name)
			return
		}
		seen[name] = true
		hasOwnerKey = hasOwnerKey || (f.Ref != nil && f.Ref.Owner)
		hasOrdinal = hasOrdinal || (f.Generator != nil && f.Generator.Name == GeneratorOrdinal)
	}
	if k.Driver == "" {
		if !hasOwnerKey || !hasOrdinal {
			v.add(join(path, "driver"), CodeUniqueKey, "set a driver column, or combine the owner key with an ordinal field")
		}
		return
	}
	if !seen[k.Driver] {
		v.add(join(path, "driver"), CodeUniqueKey, "%q is not one of the key's fields", k.Driver)
		return
	}
	driver := t.Field(k.Driver)
	if driver.Type == TypeReference {
		v.add(join(path, "driver"), CodeUniqueKey, "a reference cannot drive uniqueness")
		return
	}
	v.uniqueField(join(path, "driver"), driver, space)
}

// distinctSelects checks that an owner's children can pick distinct parents.
func (v *validator) distinctSelects(t *Table, r *RelationalSpec, spaces map[string]*big.Int) {
	if t.Owner == nil {
		return
	}
	for i := range t.Fields {
		f := &t.Fields[i]
		if f.Ref == nil || f.Ref.Select == nil || !f.Ref.Select.Distinct {
			continue
		}
		target := r.Table(f.Ref.Table)
		if target == nil || target.Count == nil {
			continue // an owned target's real row count is not known statically
		}
		if t.Owner.PerParent.Max > *target.Count {
			v.add(join(join(join("tables", t.Name), "fields"), f.Name+".select.distinct"), CodeSelect,
				"per_parent.max %d exceeds the %d rows of %s, so distinct parents can run out", t.Owner.PerParent.Max, *target.Count, target.Name)
		}
	}
}

func (v *validator) nestedLayout(r *RelationalSpec) {
	o := v.opts.Output
	if o.Format == "csv" {
		v.add("output.relational.layout", CodeFormatIncompat, "CSV has no nested layout: use normalized")
		return
	}
	root := r.Table(o.Root)
	if root == nil {
		v.add("output.relational.root", CodeFKTarget, "no table named %q", o.Root)
		return
	}
	if root.Owner != nil {
		v.add("output.relational.root", CodeFormatIncompat, "%s is owned by %s: the root must have no owner", root.Name, root.Owner.Table)
		return
	}
	size := nestedMaxBytes(root, r, v.defs)
	v.recordSize(join("tables", root.Name), size)
}
