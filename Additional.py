horses = [
    ["id", "name", "stable_id", "speed"],
    ["1", "Thunder", "10", "45"],
    ["2", "Lightning", "20", "50"],
    ["3", "Blaze", "10", "48"],
    ["4", "Spirit", "30", "42"],
    ["5", "Comet", "20", "47"],
    ["6", "Majesty", "30", "44"],
    ["7", "Storm", "10", "49"],
]

stables = [
    ["stable_id", "stable_name"],
    ["10", "Blue Ribbon"],
    ["20", "Golden Hoof"],
    ["30", "Silver Saddle"],
    ["40", "Platinum Pastures"],
]

def merge_join(table1, table2, column1, column2):
    header1 = table1[0]
    header2 = table2[0]

    idx1 = header1.index(column1)
    idx2 = header2.index(column2)

    sorted_table1 = sorted(table1[1:], key=lambda row: row[idx1])
    sorted_table2 = sorted(table2[1:], key=lambda row: row[idx2])

    output_header = header1 + [col for i, col in enumerate(header2) if i != idx2]
    result = [output_header]

    i = 0
    j = 0

    while i < len(sorted_table1) and j < len(sorted_table2):
        key1 = sorted_table1[i][idx1]
        key2 = sorted_table2[j][idx2]

        if key1 == key2:
            matches_table2 = []
            jj = j

            while jj < len(sorted_table2) and sorted_table2[jj][idx2] == key2:
                matches_table2.append(sorted_table2[jj])
                jj += 1

            ii = i
            while ii < len(sorted_table1) and sorted_table1[ii][idx1] == key1:
                for row2 in matches_table2:
                    joined_row = sorted_table1[ii] + [val for k, val in enumerate(row2) if k != idx2]
                    result.append(joined_row)
                ii += 1

            i = ii
            j = jj

        elif key1 < key2:
            i += 1
        else:
            j += 1

    return result

joined = merge_join(horses, stables, "stable_id", "stable_id")
for row in joined:
    print(row)
