Archive-System Response To Revised Questions
These questions are now largely architecture-aligned. Archive-system is an outbound-only worker-driven system, so the right integration shape is:
- worker publishes archive-origin projection/state to VPS
- VPS serves editor-facing reads from its own state
- VPS queues async archive work
- worker polls and applies that work
- worker reports completion/failure back to VPS
From the archive-system perspective, the canonical persisted edit unit is the versioned file, not a partial patch or row-level mutation.
1. Projection Publish Payload
Yes, in principle this makes sense.
Archive worker can support publishing an object-level projection payload to VPS whenever machine/curated text state changes.
The payload should be thought of as a summary/projection manifest, not necessarily the full text body of all content in every case.
A reasonable minimum shape from archive perspective is:
{
  "object_id": "OBJ-...",
  "media_type": "document|audio|video|photo|other",
  "projection_version": "string",
  "generated_at": "RFC3339 timestamp",
  "text_artifacts": [
    {
      "kind": "ocr_machine|transcript_machine|transcript_curated|captions_machine|captions_curated|description_curated|translation_machine|translation_curated",
      "version": "string",
      "is_active": true,
      "reference": {
        "type": "archive-path|artifact-key|storage-reference",
        "value": "..."
      },
      "metadata": {}
    }
  ]
}
Important archive-side note:
- archive is comfortable publishing what text artifacts exist and which one is active/current
- whether the full text bodies should also be pushed in the same payload should be decided separately based on size and VPS needs
2. Machine Output Coverage By Media Type
Current repo state:
- Document OCR text: yes
  - OCR per page is generated and stored internally in page_text
  - OCR text files are also written under ocr/
- Audio transcript: yes
  - transcript pipeline exists
  - outputs include transcript/transcript_v1.txt and transcript/transcript_v1.json
- Video transcript: yes
  - same transcript pipeline, with audio extracted from video first
- Video captions: no current support
  - no caption pipeline or caption data model exists in current repo/docs
  - but it is planned in the future
So today the answer is:
- document OCR: yes
- audio transcript: yes
- video transcript: yes
- video captions: no in current implementation
Timeline for captions: not currently committed in code/docs, so archive should not promise a date yet unless you want to make a product decision.
3. Curated File Versioning Rules
This question is aligned and important.
Archive-preferred model:
- curated outputs are stored as versioned curated files
- machine-generated outputs remain preserved
- curated outputs do not overwrite machine originals conceptually
Archive-side preference discussed so far:
- date-based curated versions are natural
- daily version bumping is acceptable
- all edits during a day can target the same dated version
Suggested rule shape:
- machine file:
  - transcript/transcript_v1.txt
- curated file:
  - transcript/transcript_curated_YYYYMMDD.txt
or equivalent naming under a curated namespace if you prefer cleaner separation.
Active/current version
- archive should explicitly mark one version as active/current
- best done through a small metadata/manifest pointer, not implicit filename guessing
So backend should expect:
- versioned curated files
- one active/current curated version
- archive may use date-based versioning rather than monotonic sequence-only versioning
4. Async Apply Support (curation_apply)
This is aligned.
Yes, this is the right shape if it is:
- async
- queue-driven
- worker-polled
- full-file replacement only
Archive perspective:
- curation_apply should replace the full content of the target curated file version
- no patch semantics
- no partial in-file mutation semantics
Idempotency
This makes sense as:
- (object_id, curated_kind, target_version)
That is a much better fit than patch-level idempotency.
Result reporting
Archive worker should be able to report:
- success
- terminal failure
- retryable failure
A minimal result shape could be:
{
  "object_id": "OBJ-...",
  "curated_kind": "transcript|captions|ocr_text|description|translation",
  "target_version": "20260413",
  "status": "completed|failed",
  "retryable": false,
  "message": "..."
}
Per-item failure reporting is less important if apply is atomic at the file level.
5. Source Reference For Apply
This question is aligned.
Since archive is outbound-only, worker should consume input from a VPS-provided secure downloadable reference, not from a direct backend callback into archive.
Preferred mechanisms, in order:
1. signed download reference / signed download URL
- best fit with current architecture
- worker downloads the curated file body securely
2. storage key only
- acceptable only if paired with a worker-accessible signed retrieval mechanism
- raw storage key alone is not enough
3. artifact id
- useful as an indirection key on VPS side, but worker still needs a secure downloadable reference
So the clean answer is:
- preferred: signed download ref
- archive worker downloads the full curated file content and writes the target curated version
6. Reprocessing After Curation Policy
This is aligned, but the policy should remain file/version-oriented.
From archive perspective, the most coherent choices are:
- keep curated active
- mark curated stale
The least archive-aligned option is:
- auto-rebase at segment level
Because archive is not trying to be a merge engine.
Recommended archive policy
- keep curated version preserved
- when machine outputs regenerate, publish a new projection_version
- mark existing curated version as stale relative to the new machine baseline if needed
- require explicit human review/re-approval rather than automatic rebasing
VPS signal should be something like:
- projection_version changed
- curated_version exists
- curated_status = current|stale
7. Optional Derived Projection Metadata
This question makes sense if understood as derived editor convenience data, not canonical persistence.
Yes, archive can plausibly publish derived structures, for example:
- transcript segments: start_ms, end_ms, speaker, text
- document pages: page_number, text
But the stability guarantee should be weak and explicit:
- these are derived projections for editing/view convenience
- they are not archive’s canonical persisted edit unit
- they may change when machine outputs are regenerated
- archive does not want to promise durable segment identity unless it explicitly adopts that model later
For documents:
- page_number is the most natural stable structure
For transcripts:
- segment structures may be published as convenience
- but archive should avoid overpromising durable segment IDs if it does not intend to store them canonically
8. Operational Constraints
This question is aligned.
Archive can answer this operationally, though current repo does not define formal p50/p95 SLOs for these future flows yet.
Current likely answer:
- file size limits: TBD by implementation
- apply latency target: best-effort async worker processing, not synchronous request SLA
- retry guidance:
  - retryable: transient download/network/storage/lease issues
  - terminal: invalid payload, unsupported kind, invalid target version, failed validation
This should probably be documented when curation_apply is specified.
9. Security/Auth Confirmation
Yes, this is aligned and confirmed.
These additions should stay within the existing worker-auth model:
- x-worker-auth-token
- queue polling
- async worker callbacks/results
- no inbound archive endpoints
That matches current architecture and should remain a hard constraint.
10. Rollout Plan
This is aligned.
From current repo state, a realistic rough split is:
Phase 1
- projection publish payload for text artifact existence/version metadata
- confirm machine output coverage
- curated file versioning convention
- curation_apply queue action for full-file replacement
- signed-download input reference for apply
- security/auth unchanged
Phase 2
- richer projection publish payloads
- stale/current signaling after reprocessing
- optional derived page/segment editor projections
- more formal operational limits/SLOs
- caption support, if product chooses to add it
Not currently phaseable without more design
- durable transcript/caption segment identity guarantees
- auto-rebase semantics across regenerated machine output
---
Small Adjustments I’d Recommend To Backend Wording
1. In question 1, prefer:
- “projection manifest/payload”
instead of implying it must always include full text bodies
2. In question 6, prefer:
- “keep curated preserved + optionally mark stale”
rather than “auto-rebase/bump”
3. In question 7, explicitly say:
- “derived editor convenience projection”
to avoid confusion with archive’s canonical persistence model
---
Bottom Line
These revised questions now mostly make sense for archive-system.
The main archive-side positions are:
- canonical persisted edit unit is the file
- curated outputs should be versioned curated files
- apply should be async full-file replacement
- worker can publish projection/version metadata
- derived page/segment structures are fine as convenience projections
- captions are not currently implemented
- no inbound archive endpoints should be introduced
