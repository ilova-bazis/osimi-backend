import type { MediaKind } from "./capabilities.ts";
import type {
  IngestItemKind,
  IngestionClassificationType,
} from "../../validation/ingestion.ts";

const ALL_ITEM_KINDS: readonly IngestItemKind[] = [
  "photo",
  "audio",
  "video",
  "scanned_document",
  "document",
  "other",
];

const ALL_MEDIA_KINDS: readonly MediaKind[] = [
  "image",
  "audio",
  "video",
  "document",
];

const ALLOWED_ITEM_KINDS_BY_CLASSIFICATION_TYPE: Record<
  IngestionClassificationType,
  readonly IngestItemKind[]
> = {
  newspaper_article: ["scanned_document", "document"],
  magazine_article: ["scanned_document", "document"],
  book_chapter: ["scanned_document", "document"],
  book: ["scanned_document", "document"],
  letter: ["scanned_document", "document"],
  speech: ["audio", "video", "scanned_document"],
  interview: ["audio", "video", "scanned_document"],
  report: ["scanned_document", "document"],
  manuscript: ["scanned_document", "document"],
  image: ["photo"],
  document: ["scanned_document", "document"],
  other: ALL_ITEM_KINDS,
};

const ALLOWED_MEDIA_KINDS_BY_ITEM_KIND: Record<
  IngestItemKind,
  readonly MediaKind[]
> = {
  photo: ["image"],
  audio: ["audio"],
  video: ["video"],
  scanned_document: ["image", "document"],
  document: ["document"],
  other: ALL_MEDIA_KINDS,
};

export function getAllowedItemKindsForClassificationType(
  classificationType: IngestionClassificationType,
): readonly IngestItemKind[] {
  return ALLOWED_ITEM_KINDS_BY_CLASSIFICATION_TYPE[classificationType];
}

export function getAllowedMediaKindsForItemKind(
  itemKind: IngestItemKind,
): readonly MediaKind[] {
  return ALLOWED_MEDIA_KINDS_BY_ITEM_KIND[itemKind];
}

export function isClassificationTypeCompatibleWithItemKind(params: {
  classificationType: IngestionClassificationType;
  itemKind: IngestItemKind;
}): boolean {
  return getAllowedItemKindsForClassificationType(
    params.classificationType,
  ).includes(params.itemKind);
}

export function isItemKindCompatibleWithMediaKind(params: {
  itemKind: IngestItemKind;
  mediaKind: MediaKind;
}): boolean {
  return getAllowedMediaKindsForItemKind(params.itemKind).includes(
    params.mediaKind,
  );
}
