def mymap(stopas):
    # Keep marsrutas as required, but tolerate missing numeric or zone values.
    marsrutas = None
    svoris = 0.0
    siuntu_skaicius = 0
    geografine_zona = None

    parstrings = stopas.split("}{")
    for parstring in parstrings:
        # Skip only malformed fragments, not the whole stop.
        if "=" not in parstring:
            continue

        vardas, reiksme = parstring.split("=", 1)
        if vardas == "marsrutas" and reiksme != "":
            marsrutas = reiksme
        if vardas == "svoris" and reiksme != "":
            try:
                svoris = float(reiksme)
            except ValueError:
                svoris = 0.0
        if vardas == "siuntu skaicius" and reiksme != "":
            try:
                siuntu_skaicius = int(reiksme)
            except ValueError:
                siuntu_skaicius = 0
        if vardas == "geografine zona" and reiksme != "":
            geografine_zona = reiksme

    if marsrutas is not None:
        zonos = {}
        if geografine_zona is not None:
            zonos[geografine_zona] = 1

        return (marsrutas, (svoris, siuntu_skaicius, zonos))

def build_parsed(rawdata):
    return (
        rawdata.flatMap(
            lambda line: []
            if len(line.strip()) < 4
            else line.strip()[2 : len(line.strip()) - 2].split("}}{{")
        )
        .map(mymap)
        .filter(lambda d: d is not None)
        .cache()
    )
