# Parcel Route Aggregation (Lab 2, Variant 4)

This project processes semi-structured parcel delivery stop records with Apache Spark (RDD API) and aggregates results by `marsrutas` (route).

For each route, the final output contains:
- total parcel weight (`svoris`)
- total parcel count (`siuntu skaicius`)
- pivoted zone frequencies (`geografine zona` as columns such as `Z1`, `Z2`, `Z3`)

## Assigned Variant

Variant is calculated by:

`(n mod 4) + 1`

For this work:
- `n = 19`
- `(19 mod 4) + 1 = 4`

So the assigned task is **Variant 4**:
1. Sum `svoris` by `marsrutas`
2. Sum `siuntu skaicius` by `marsrutas`
3. Pivot `geografine zona` into separate columns and count occurrences

## Main Files

- [Task description PDF](Laboratory%20Work%202.pdf)
- [Task report PDF](report.pdf)
- [Pipeline runner](main.py)
- [Mapper / parser](map.py)
- [Reducer](red.py)
- [Sorting + pivot column prep](Sort.py)
- [Input archive (subset)](duom_cut.7z)
- [Input archive (full)](duom_full.7z)

## Processing Logic

1. Read input text file (`duom_cut.txt`) as RDD.
2. Split each line into stop fragments.
3. Parse each fragment and emit:
   - key: `marsrutas`
   - value: `(svoris, siuntu_skaicius, {geografine_zona: 1})`
4. Reduce by key:
   - sum weights
   - sum parcel counts
   - merge zone counters
5. Collect distinct zones and print pivoted tabular output.

## How To Run

1. Extract input file:

```bash
tar -xf duom_cut.7z
```

2. Run Spark job:

```bash
spark-submit main.py
```

If you want to save output:

```bash
spark-submit main.py > redout.txt
```

## Example Output (`duom_cut.txt`)

| marsrutas | svoris_sum | siuntu_skaicius_sum | Z1 | Z3 |
|---|---:|---:|---:|---:|
| 102 | 23012.6 | 1986 | 331 | 5 |
| 103 | 6787 | 182 | 37 | 37 |
| 104 | 810.61 | 129 | 59 | 0 |

## Notes

- The parser is tolerant to incomplete or malformed numeric fields:
  - missing/invalid `svoris` is treated as `0.0`
  - missing/invalid `siuntu skaicius` is treated as `0`
- Records without `marsrutas` are skipped, because route key is required for grouping.

## Summary

The implementation completes Lab 2 Variant 4 using Spark RDD transformations and actions, producing a compact per-route operational view with both totals and zone distribution.
