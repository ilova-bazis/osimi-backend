import { z } from "zod";

import { mapZodErrorToValidation } from "./zod-errors.ts";

const confidenceSchema = z.enum(["low", "medium", "high"]);

const dateValueSchema = z
  .string()
  .regex(/^\d{4}(-\d{2})?(-\d{2})?$/, {
    message: "date value must be YYYY, YYYY-MM, or YYYY-MM-DD",
  })
  .nullable();

const dateBlockSchema = z.strictObject({
  value: dateValueSchema,
  approximate: z.boolean(),
  confidence: confidenceSchema,
  note: z.string().nullable(),
});

export const ingestionSummarySchema = z
  .strictObject({
    title: z.strictObject({
      primary: z.string().trim().min(1),
      original_script: z.string().nullable(),
      translations: z.array(
        z.strictObject({
          lang: z.string().trim().min(1),
          text: z.string().trim().min(1),
        }),
      ),
    }),
    classification: z.strictObject({
      tags: z.array(z.string()),
      summary: z.string().nullable(),
    }),
    dates: z.strictObject({
      published: dateBlockSchema,
      created: dateBlockSchema,
    }),
    processing: z
      .strictObject({
        ocr_text: z
          .strictObject({
            enabled: z.boolean(),
            language: z.string().trim().min(1).optional(),
          })
          .optional(),
        audio_transcript: z
          .strictObject({
            enabled: z.boolean(),
            language: z.string().trim().min(1).optional(),
          })
          .optional(),
        video_transcript: z
          .strictObject({
            enabled: z.boolean(),
            language: z.string().trim().min(1).optional(),
          })
          .optional(),
      })
      .optional(),
    publication: z
      .strictObject({
        name: z.string().nullable().optional(),
        issue: z.string().nullable().optional(),
        volume: z.string().nullable().optional(),
        pages: z.string().nullable().optional(),
        place: z.string().nullable().optional(),
      })
      .optional(),
    people: z
      .strictObject({
        subjects: z.array(z.string()).optional(),
        authors: z.array(z.string()).optional(),
        contributors: z.array(z.string()).optional(),
        mentioned: z.array(z.string()).optional(),
      })
      .optional(),
    links: z
      .strictObject({
        related_object_ids: z.array(z.string()).optional(),
        external_urls: z.array(z.string()).optional(),
      })
      .optional(),
    notes: z
      .strictObject({
        internal: z.string().nullable().optional(),
        public: z.string().nullable().optional(),
      })
      .optional(),
  })
  .superRefine((value, context) => {
    const uniqueTags = new Set(value.classification.tags);
    if (uniqueTags.size !== value.classification.tags.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "classification.tags must contain unique values.",
        path: ["classification", "tags"],
      });
    }
  });

export type IngestionSummary = z.infer<typeof ingestionSummarySchema>;

export function parseIngestionSummary(value: unknown): IngestionSummary {
  const parsed = ingestionSummarySchema.safeParse(value);
  if (!parsed.success) {
    throw mapZodErrorToValidation(parsed.error);
  }

  return parsed.data;
}
