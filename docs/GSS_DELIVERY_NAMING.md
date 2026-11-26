# GSS delivery file names

Deniz, verbatim: "remember the delivery names system ... we need to stick with that", and earlier: "keep the old file names agnostic to the stitching tool ... the files should just be stitched and delivered".

The project lead's rule: every served path is a delivery name. That covers original parts, restored and newly stitched parts, and every assembly (V85, V79, V49, strict, reviewed and clock-join). No tool, batch, version or time range ever appears in a name.

## The spec

The spec is the live catalog's own `naming` (`public/catalog.json`, the `naming` object). `delivery_naming.NAMING_SPEC` holds that text. The builder refuses a catalog whose spec differs from it.

```
<id>_<Continent>_<Country>_<City>_<Plaza>/<Month>/<Weekday>/
  <id>_<Plaza>_<YYYY>_<Month>_W<1-5>_<Weekday>_hour_<01-12>[_part_NN].mp4
```

| Part | Rule |
|---|---|
| `id` | The Stoarama recording_id, exactly as selected in the site. No plaza_id substitution or padding. |
| `Plaza` | The stream's catalog `metadata.plaza_name`. When the catalog has none, its display `name` is used instead ("verified stream display plaza name retained"). |
| `Continent`, `Country`, `City` | Catalog metadata. An absent field is written `Unresolved_Continent`, `Unresolved_Country` or `Unresolved_City`. |
| `Month`, `Weekday` | The English names for the stream-local date. The weekday folder matches the weekday in the file name. |
| `W` | Calendar-month week: days 1–7 are W1, 8–14 W2, 15–21 W3, 22–28 W4, 29–31 W5. W is what keeps one weekday folder unique across weeks. |
| `hour` | The scheduled slot ordinal, 01–12. Delivery hour 1 is local 08:00–09:00 and 12 is local 19:00–20:00. It is not the clock hour, and the clock range is shown separately on the page. |
| `_part_NN` | An hour with one current file is plain `_hour_XX.mp4`. A fragmented hour numbers its current files `_part_01`, `_part_02`, … in source order, and an assembly takes the place of its first part. A file keeps its number while other files of its hour are not yet delivered, so numbers can skip. |
| Sanitation | Unicode NFC. Letters and digits are kept, spaces become underscores, and punctuation other than hyphen and dot is removed. Traversal, empty and reserved (`con`, `aux`, …) tokens are refused. |
| Uniqueness | Every path is unique under Unicode NFC case-fold. |

A stream the catalog already names (`folder`) must get the same folder back from these rules. Otherwise the build fails.

## The one implementation

[`backend/scripts/gss/delivery_naming.py`](../backend/scripts/gss/delivery_naming.py) is the only generator and validator. Nothing else names delivered files.

- `catalog_names(catalog)` returns the folder and file stem for each stream, after checking the catalog's spec.
- `delivery_path(names, recording_id, local_date, delivery_hour, ordinal, count)` returns the name of the ordinal-th of an hour's `count` current files.
- `check_paths(names, files)` is the hard gate. Each path must be a delivery name for its stream and date. An hour must be either one plain file or increasing numbered parts. Paths must be unique under NFC case-fold.

It is used by:

- `best14_ledger.py`, which names every served file and refuses a selection whose paths fail `check_paths`;
- `build_generation.py --ledger`, which publishes those names. It renames a file from the parent generation whose path is not its delivery name, and checks every published path again;
- `viewer_smoke.py` and `download_smoke.py`, the republish gates, offline on a build and with `--current` against the live page.

## Clock-join (PR #625)

`clockjoin.AssemblyPath` names a whole-hour run with the hour's plain name, and any other run with its first input's existing path. The builder ignores that name: it renames every served file at the delivery-path level, including clock-join assemblies, which enter through `best14_ledger.py --clock-proofs`. Before #625 publishes on its own, make these two changes there:

1. Its `build_generation.py --clock-assemblies` path must not set `path` from `AssemblyPath`. Publish clock-join assemblies through `--ledger` instead.
2. Better still, drop `AssemblyPath`, and name the assembly with `delivery_naming.delivery_path` at its place among the hour's current files, renumbering the hour's later parts.
