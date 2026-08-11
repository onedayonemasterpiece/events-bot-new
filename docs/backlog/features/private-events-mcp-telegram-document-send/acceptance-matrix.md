# Acceptance matrix — Telegram message document v1

## A. Contract and discovery

| ID | Check | Expected |
|---|---|---|
| A-01 | File-send flag absent/false | Tool catalog and behavior remain image-only as before. |
| A-02 | File-send true, media-story false | `social_asset_stage/status` are discoverable; preview/stories are not. |
| A-03 | Role schema under file-send | `document` advertised; unsupported roles absent. |
| A-04 | VK + document | Rejected before provider call. |
| A-05 | Document with action other than `send_message` | Rejected before prepare persistence/provider call. |
| A-06 | Two documents or mixed image+document | Rejected. |
| A-07 | Target without `send_message` | `document` not advertised and prepare rejected. |

## B. Ingress and immutable storage

| ID | Check | Expected |
|---|---|---|
| B-01 | APK with valid Android ZIP structure | Ready asset with detected APK MIME, safe `.apk` name, size and SHA-256. |
| B-02 | APK filename on ordinary ZIP | `FILE_TYPE_MISMATCH`. |
| B-03 | PDF, ZIP, TXT/MD/CSV/JSON, DOCX/XLSX/PPTX | Accepted only when bytes match policy. |
| B-04 | Unsupported opaque binary/EXE/ELF | `FILE_TYPE_NOT_ALLOWED`. |
| B-05 | Declared MIME mismatch | `FILE_TYPE_MISMATCH`; `application/octet-stream` hint may classify safely. |
| B-06 | Oversize/empty/truncated file | Stable rejection, no retained partial asset. |
| B-07 | Symlink/directory/device/non-regular file | `FILE_INTEGRITY_FAILED`. |
| B-08 | ZIP traversal, encrypted ZIP, excessive entries/ratio/declared size | Stable rejection without extraction. |
| B-09 | Filename path separators, controls, bidi override, reserved names, excessive length | Sanitized bounded display name; no path traversal. |
| B-10 | Mutate bytes after stage | Prepare/commit rehash fails before Telegram call. |
| B-11 | Expired asset | Commit fails before Telegram call. |
| B-12 | Cross-principal or cross-provider asset reuse | Rejected. |
| B-13 | Input is a string path/URL instead of fileParams object | Existing `FILE_REF_UNRESOLVED`/invalid-arguments path; never fetched. |

## C. Authorization and safety

| ID | Check | Expected |
|---|---|---|
| C-01 | Stage with `telegram:dm:send` | Allowed for document. |
| C-02 | Stage with read-only or story-only scope | Denied. |
| C-03 | Prepare/commit scope removed after staging | Denied; staging grants no send authority. |
| C-04 | File-send kill switch disabled after prepare | Commit denied before provider call. |
| C-05 | Provider/target rights change after prepare | Commit denied before provider call. |
| C-06 | Action digest changes filename/digest/size/MIME/target/caption | Mismatch rejected. |
| C-07 | Audit/log/response inspection | No signed URL, original file ID, internal path, access hash, token, native peer/file ID. |
| C-08 | Existing approval/idempotency flow | One authorization consumption and at most one Telegram attempt. |

## D. Telegram adapter

| ID | Check | Expected |
|---|---|---|
| D-01 | Saved Messages + document, no caption | One `send_file`, forced document, correct display filename. |
| D-02 | Saved Messages + document + rich caption | Caption/entities preserved. |
| D-03 | Resolved user/group with send rights | Same typed path succeeds. |
| D-04 | Read-only channel | No document capability and no provider call. |
| D-05 | Telethon timeout | `outcome_unknown`, `retry_safe=false`, no blind resend. |
| D-06 | Definite provider rejection | Durable failed status with sanitized error code. |
| D-07 | Read-after-write | Correct target/message, document present, filename and size checked when available. |
| D-08 | Provider call arguments | No download URL/native method selector/raw MCP provider args. |

## E. Regression

| ID | Check | Expected |
|---|---|---|
| E-01 | Existing image stage/status/preview/story tests | Unchanged PASS. |
| E-02 | Text-only send_message | Unchanged PASS. |
| E-03 | Edit/delete/forward/reaction/schedule | Unchanged PASS. |
| E-04 | VK social workspace | Unchanged PASS; no document advertisement. |
| E-05 | OAuth legacy provider-level scope compatibility | Unchanged within existing read/write boundary. |
| E-06 | Disabled MCP startup with malformed stale file-send settings | Remains inert as current config policy requires. |

## F. Live acceptance

| ID | Check | Expected |
|---|---|---|
| F-01 | Real ChatGPT fileParams -> Saved Messages with deterministic APK fixture | Actual downloadable Telegram document, not link. |
| F-02 | MCP response | `succeeded`, opaque item/operation refs, read-after-write verified. |
| F-03 | Telegram UI | Filename and caption match sanitized frozen preview. |
| F-04 | Storage cleanup | Asset expires/deletes within configured TTL; no orphaned partial files. |
| F-05 | Rollback probe | Disabling file-send immediately removes document capability without breaking text/image. |
