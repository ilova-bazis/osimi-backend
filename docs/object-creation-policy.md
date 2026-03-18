# Object Creation Policy

## Purpose

This policy defines how uploaded files are translated into **archival objects** during ingestion.

The goal is to ensure that:

* objects represent **distinct archival items**
* files represent **digital representations of those items**
* ingestion remains a **submission container**, not the archival unit.

---

# Core Principles

### 1. Ingestion is a submission container

An **ingestion** represents a user submission or batch upload.

* A single ingestion may contain **one or many uploaded files**.
* An ingestion may produce **one or many archival objects**.

Ingestion is a **workflow boundary**, not an archival identity.

---

### 2. Objects represent archival items

An **object** represents a **distinct archival item**.

Examples of archival items include:

* a photograph
* a document or letter
* an audio recording
* a video recording
* a book or notebook
* a newspaper issue or clipping

Objects are the primary units for:

* cataloging
* metadata
* search
* citation
* access control

---

### 3. Files are representations of objects

Uploaded files represent **digital manifestations of an object**.

Examples include:

* master scans
* alternate encodings
* access copies
* thumbnails
* OCR text
* transcripts
* subtitles
* multiple sides or pages of an item

An object may contain **multiple files**.

---

# Object Creation Rule

During ingestion processing:

> The system must create **one object per distinct archival item**, not one object per ingestion and not necessarily one object per file.

Multiple uploaded files may belong to the same object **only when they represent the same original item**.

---

# When to Create Separate Objects

Create **separate objects** when uploaded files represent **different archival items**.

Examples:

* multiple photographs
* separate letters
* different audio interviews
* different videos or recordings
* unrelated newspaper clippings

Example:

User uploads:

```
photo1.jpg
photo2.jpg
photo3.jpg
```

Result:

```
1 ingestion
3 objects
```

---

# When to Group Files Into One Object

Files should belong to the **same object** when they represent **the same original item**.

Examples include:

* front and back of a photograph
* multiple pages of a document
* TIFF master + JPEG access copy
* WAV master + MP3 derivative
* video + subtitles + transcript
* document scans + OCR text

Example:

User uploads:

```
letter_page_1.tif
letter_page_2.tif
letter_page_3.tif
letter.pdf
letter_ocr.txt
```

Result:

```
1 ingestion
1 object
5 files
```

---

# Media-Type Guidelines

## Photos

**Object boundary:** one photograph

Separate objects:

* different photos

Same object:

* front/back of same photo
* TIFF + JPEG versions of same photo

Example:

```
family_photo_front.tif
family_photo_back.tif
family_photo_access.jpg
```

Result:

```
1 object
3 files
```

---

## Documents

**Object boundary:** one document or written work

Separate objects:

* different letters
* different articles
* separate manuscripts

Same object:

* multiple page scans
* PDF + page images
* OCR text

Example:

```
article_page_1.tif
article_page_2.tif
article_page_3.tif
article.pdf
article_ocr.txt
```

Result:

```
1 object
5 files
```

---

## Audio

**Object boundary:** one recording or program

Separate objects:

* different interviews
* separate recordings

Same object:

* WAV + MP3 versions
* transcript files

Example:

```
interview.wav
interview.mp3
interview_transcript.txt
```

Result:

```
1 object
3 files
```

---

## Video

**Object boundary:** one recording or work

Separate objects:

* different videos or events

Same object:

* master video + access copy
* subtitles
* transcripts

Example:

```
lecture_master.mov
lecture_access.mp4
lecture_subtitles.srt
lecture_transcript.txt
```

Result:

```
1 object
4 files
```

---

# Practical Decision Rule

Use the following interpretation guide:

Files belong to the **same object** if a cataloger would describe them as **the same item**.

Create **separate objects** if a cataloger would assign **separate titles, dates, or descriptions**.

---

# Summary

The system follows these structural rules:

```
ingestion
   → objects
        → files
```

* One ingestion may produce **many objects**.
* One object may contain **many files**.
* Files are grouped into the same object **only when they represent the same original archival item**.
