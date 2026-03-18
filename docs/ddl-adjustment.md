This is a **good proposal** overall. It is thoughtful, incremental, and mostly aligned with the direction you want.

My take is:

**architecturally: yes**
**DDL-wise: almost there**
**a few parts I would tighten before shipping**

The strongest parts are:

* introducing `ingestion_items`
* introducing `ingestion_item_files`
* pivoting `objects` provenance from ingestion-level uniqueness to item-level uniqueness
* adding `ingestion_item_id` to `object_events`

Those are the right moves.

---

# What I like

## 1. `ingestion_items` is the correct missing layer

This is the core fix. It gives you:

* one ingestion -> many logical items
* one item -> one future object
* room for per-item metadata overrides

That is exactly the right boundary.

## 2. `ingestion_item_files` is enough, no group table needed

Yes — with this table, grouping is already modeled:

* membership
* role
* ordering inside the item

So your earlier instinct about not needing a separate group table is correct.

## 3. Provenance pivot on `objects` is right

This part is important and correct:

This is a **good proposal** overall. It is thoughtful, incremental, and mostly aligned with the direction you want.

My take is:

**architecturally: yes**
**DDL-wise: almost there**
**a few parts I would tighten before shipping**

The strongest parts are:

* introducing `ingestion_items`
* introducing `ingestion_item_files`
* pivoting `objects` provenance from ingestion-level uniqueness to item-level uniqueness
* adding `ingestion_item_id` to `object_events`

Those are the right moves.

---

# What I like

## 1. `ingestion_items` is the correct missing layer

This is the core fix. It gives you:

* one ingestion -> many logical items
* one item -> one future object
* room for per-item metadata overrides

That is exactly the right boundary.

## 2. `ingestion_item_files` is enough, no group table needed

Yes — with this table, grouping is already modeled:

* membership
* role
* ordering inside the item

So your earlier instinct about not needing a separate group table is correct.

## 3. Provenance pivot on `objects` is right

This part is important and correct:

* drop `objects_source_ingestion_unique_idx`
* add `objects.source_ingestion_item_id`
* add unique index on `source_ingestion_item_id`

That cleanly changes the rule from:

* one ingestion -> one object

to:

* one ingestion -> many objects
* one item -> at most one object

That is the correct invariant.

## 4. Adding item reference to `object_events` is very useful

Good call. Without this, item-level failures and progress would get muddy.

---

# What I would change

## 1. `item_index >= 0` should probably be `> 0`

You wrote:

```sql
item_index integer NOT NULL CHECK (item_index >= 0)
```

I would make it:

```sql
CHECK (item_index > 0)
```

Reason:

* your UI and worker logic will almost certainly think in 1-based ordering
* `0` is usually not helpful in user-visible ordering
* it keeps it consistent with `page_number > 0`

So I would use positive ordering for both item and file ordering unless you have a very specific reason for zero-based indexing.

---

## 2. I would rename `source_order` to `sort_order`

Your current name:

```sql
source_order integer
```

works, but `sort_order` is clearer.

Why:

* “source order” can sound like original upload order or source-system order
* “sort order” clearly means display/processing order inside the item
* it matches common relational naming conventions

This is not a functional issue, but it will reduce confusion later.

If you want to preserve the nuance that it reflects the original source sequence, you can keep `source_order`, but I think `sort_order` is the better schema name.

---

## 3. The default-collision concern is valid, but nullable ordering has tradeoffs

You made `source_order` nullable to avoid uniqueness collisions before explicit assignment. That is reasonable.

But it also means:

* items can exist with no deterministic file order
* worker logic must fallback to `created_at` or `id`
* some item types absolutely require ordering

For example:

* page sets
* front/back pairs
* side A / side B

So I would choose one of these two approaches:

### Option A — keep nullable, but enforce in application

This is okay if:

* UI always sets it before submission
* backend validates that submitted items requiring order have complete order

### Option B — make it required and explicit

For archival workflows, I slightly prefer this:

```sql
sort_order integer NOT NULL CHECK (sort_order > 0)
UNIQUE (ingestion_item_id, sort_order)
```

because then every membership row is deterministic from birth.

My honest opinion: if your UI/backend already knows insertion order, it is better to store it immediately and make it non-null.

---

## 4. `is_primary` needs a uniqueness rule if you care about single-primary semantics

Right now this allows multiple rows in one item to have `is_primary = true`.

If that is acceptable, fine.
But if the meaning is “the primary file within this item,” add:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS ingestion_item_files_one_primary_per_item_idx
  ON ingestion_item_files (ingestion_item_id)
  WHERE is_primary = true;
```

That gives you at most one primary file per item.

I would add it.

---

## 5. The shadow `ingestion_id` on `ingestion_item_files` is strong, but a bit heavy

This is the most interesting part of your proposal.

You added `ingestion_id` to `ingestion_item_files` so you can enforce cross-table consistency via composite FKs. That is a **valid and robust** technique.

What it gives you:

* prevents linking a file from ingestion A to an item in ingestion B at the DB level
* very strong integrity

That is good.

What it costs:

* denormalization
* backfill/migration complexity
* one more column that application code must keep consistent
* more verbose inserts

So the question is whether you want:

* **strict DB-enforced integrity**
  or
* **simpler schema with app-level validation**
