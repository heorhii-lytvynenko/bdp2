def sort_result(parsed, result):
    visos_zonos = sorted(parsed.flatMap(lambda x: x[1][2].keys()).distinct().collect())
    rows = result.sortBy(
        lambda item: (0, int(item[0])) if item[0].isdigit() else (1, item[0])
    ).collect()
    return visos_zonos, rows
