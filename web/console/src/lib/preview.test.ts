import { test } from "node:test";
import assert from "node:assert/strict";
import { decidePreview, looksLikeSseCError, TEXT_PREVIEW_LIMIT, MEDIA_PREVIEW_LIMIT } from "./preview.js";

test("small text is previewable", () => {
  assert.deepEqual(decidePreview({ contentType: "text/plain", size: 100 }), { kind: "text" });
  assert.deepEqual(decidePreview({ contentType: "application/json", size: 10 }), { kind: "text" });
  assert.deepEqual(decidePreview({ contentType: "", size: 10, key: "a/b/readme.md" }), { kind: "text" });
});

test("image and pdf are previewable", () => {
  assert.deepEqual(decidePreview({ contentType: "image/png", size: 1000 }), { kind: "image" });
  assert.deepEqual(decidePreview({ contentType: "", size: 1000, key: "pic.svg" }), { kind: "image" });
  assert.deepEqual(decidePreview({ contentType: "application/pdf", size: 1000 }), { kind: "pdf" });
});

test("over-limit text falls back to download", () => {
  assert.deepEqual(decidePreview({ contentType: "text/plain", size: TEXT_PREVIEW_LIMIT + 1 }), {
    kind: "download",
    reason: "over-limit",
  });
});

test("over-limit media falls back to download", () => {
  assert.deepEqual(decidePreview({ contentType: "image/png", size: MEDIA_PREVIEW_LIMIT + 1 }), {
    kind: "download",
    reason: "over-limit",
  });
});

test("unknown binary type falls back to download", () => {
  assert.deepEqual(decidePreview({ contentType: "application/octet-stream", size: 10 }), {
    kind: "download",
    reason: "unsupported-type",
  });
});

test("SSE-C objects are never previewed", () => {
  assert.deepEqual(decidePreview({ contentType: "text/plain", size: 1, isSseC: true }), { kind: "sse-c" });
  assert.deepEqual(decidePreview({ contentType: "image/png", size: 1, isSseC: true }), { kind: "sse-c" });
});

test("SSE-C read failure message is recognized", () => {
  assert.ok(
    looksLikeSseCError("HeadObject b/k: HTTP 400: The object was stored using a form of Server Side Encryption."),
  );
  assert.ok(!looksLikeSseCError("NoSuchBucket: bucket x"));
});
