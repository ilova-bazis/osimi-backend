export const MEDIA_KINDS = ["image", "audio", "video", "document"] as const;
export type MediaKind = (typeof MEDIA_KINDS)[number];

export const MIME_ALIASES: Record<string, string> = {
  "image/jpg": "image/jpeg",
  "image/tif": "image/tiff",
  "image/x-png": "image/png",
  "image/svg": "image/svg+xml",
  "image/pjpeg": "image/jpeg",
  "audio/mp3": "audio/mpeg",
  "audio/x-wav": "audio/wav",
  "audio/vnd.wave": "audio/wav",
  "audio/x-m4a": "audio/mp4",
  "audio/mid": "audio/midi",
  "video/x-m4v": "video/mp4",
  "video/x-msvideo": "video/avi",
  "application/x-pdf": "application/pdf",
  "application/x-zip-compressed": "application/zip",
  "application/x-gzip": "application/gzip",
  "text/xml": "application/xml",
};

export const MIME_ALLOWLIST: Record<MediaKind, string[]> = {
  image: [
    "image/jpeg",
    "image/png",
    "image/tiff",
    "image/webp",
    "image/gif",
    "image/bmp",
    "image/heic",
    "image/heif",
    "image/svg+xml",
  ],
  audio: [
    "audio/mpeg",
    "audio/wav",
    "audio/flac",
    "audio/aac",
    "audio/ogg",
    "audio/opus",
    "audio/mp4",
  ],
  video: [
    "video/mp4",
    "video/mpeg",
    "video/quicktime",
    "video/webm",
    "video/ogg",
    "video/avi",
  ],
  document: [
    "application/pdf",
    "text/plain",
    "text/rtf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  ],
};

export const EXTENSION_ALLOWLIST: Record<MediaKind, string[]> = {
  image: [
    "jpg",
    "jpeg",
    "png",
    "tif",
    "tiff",
    "webp",
    "gif",
    "bmp",
    "heic",
    "heif",
    "svg",
  ],
  audio: [
    "mp3",
    "wav",
    "flac",
    "aac",
    "ogg",
    "opus",
    "m4a",
  ],
  video: [
    "mp4",
    "m4v",
    "mpeg",
    "mov",
    "webm",
    "ogv",
    "avi",
  ],
  document: [
    "pdf",
    "txt",
    "rtf",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "ppt",
    "pptx",
  ],
};

const MIME_SETS: Record<MediaKind, Set<string>> = {
  image: new Set(MIME_ALLOWLIST.image),
  audio: new Set(MIME_ALLOWLIST.audio),
  video: new Set(MIME_ALLOWLIST.video),
  document: new Set(MIME_ALLOWLIST.document),
};

export function normalizeMime(value: string): string {
  const normalized = value.trim().toLowerCase();
  return MIME_ALIASES[normalized] ?? normalized;
}

export function getMediaKindForMime(value: string): MediaKind | undefined {
  const normalized = normalizeMime(value);

  if (MIME_SETS.image.has(normalized)) {
    return "image";
  }

  if (MIME_SETS.audio.has(normalized)) {
    return "audio";
  }

  if (MIME_SETS.video.has(normalized)) {
    return "video";
  }

  if (MIME_SETS.document.has(normalized)) {
    return "document";
  }

  return undefined;
}
