def merge_values(x, y):
    # Merge two partial aggregates of the same marsrutas.
    zonos = dict(x[2])
    for zona, kiekis in y[2].items():
        zonos[zona] = zonos.get(zona, 0) + kiekis

    return (x[0] + y[0], x[1] + y[1], zonos)


def print_result(rows, zone_columns):
    print("\t".join(["marsrutas", "svoris_sum", "siuntu_skaicius_sum"] + zone_columns))
    for marsrutas, values in rows:
        row = [marsrutas, format(values[0], "g"), str(values[1])]
        for zona in zone_columns:
            row.append(str(values[2].get(zona, 0)))
        print("\t".join(row))
