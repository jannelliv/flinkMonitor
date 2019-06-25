#!/usr/bin/env python3

configurations = {
    "star-neg": [
        ["A,0", "B,0", "C,0"],
        ["A,1"]
    ],
    "linear-neg": [
        ["A,1", "B,0"],
        ["B,1", "C,0"]
    ],
    "triangle-neg": [
        ["A,0", "C,1"],
        ["A,1", "B,0"]
    ]
}

heavy_hitters = {
    4: [1, 2],
    8: [1, 2, 3],
    16: [1, 2, 3, 4]
}

C_shift = 1000000

for formula, sets in configurations.items():
    for include_sets in range(1, len(sets) + 1):
        for processors, heavy in heavy_hitters.items():
            file_name = "heavy_{}_{}_h{}.csv".format(processors, formula, include_sets)
            output = open(file_name, 'x')
            for this_set in sets[:include_sets]:
                for prefix in this_set:
                    for value in heavy:
                        if prefix.startswith("C,"):
                            value += C_shift
                        output.write(prefix + "," + str(value) + "\n")
            output.close()
