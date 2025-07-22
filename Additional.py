import math


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

def aggregate_sum(table, column):

    header = table[0]
    idx = header.index(column)
    total = 0.0

    for row in table[1:]:
        total += float(row[idx])
    return total

def aggregate_avg(table, column):

    header = table[0]
    idx = header.index(column)
    total = 0.0
    counter = 0

    for row in table[1:]:
        total += float(row[idx])
        counter += 1
    return total / counter if counter > 0 else None

def aggregate_min(table, column):

    header = table[0]
    idx = header.index(column)
    min_value = None

    for row in table[1:]:
        val = float(row[idx])
        if min_value is None or val < min_value:
            min_value = val
    return min_value

def aggregate_max(table, column):

    header = table[0]
    idx = header.index(column)
    max_value = None

    for row in table[1:]:
        val = float(row[idx])
        if max_value is None or val > max_value:
            max_value = val
    return max_value

def scalar_abs(table, column):
    header = table[0]
    idx = header.index(column)

    result = [header[:]]

    for row in table[1:]:
        new_row = row[:]
        val = float(new_row[idx])
        new_row[idx] = str(abs(val))
        result.append(new_row)

    return result

def scalar_sqrt(table, column):
    header = table[0]
    idx = header.index(column)

    result = [header[:]]

    for row in table[1:]:
        new_row = row[:]
        val = float(new_row[idx])
        new_row[idx] = str(math.sqrt(val))
        result.append(new_row)

    return result

def scalar_pow(table, column, power):
    header = table[0]
    idx = header.index(column)

    result = [header[:]]

    for row in table[1:]:
        new_row = row[:]
        val = float(new_row[idx])
        new_row[idx] = str(pow(val, power))
        result.append(new_row)

    return result

joined = merge_join(horses, stables, "stable_id", "stable_id")
print("Horses and Their Stables")
print("-" * 40)
for row in joined[1:]:
    horse_id, horse_name, stable_id, speed, stable_name = row
    print(f"Horse '{horse_name}' (ID: {horse_id}) gallops at speed {speed} km/h, thats pretty cool if u ask me. they stay in the very stayable stable '{stable_name}'")
print("-" * 40)
print()

print("Speed Aggregation Stats")

print(f"Total combined speed of all horses: {aggregate_sum(horses, 'speed')} km/h")

print(f"Average speed: {aggregate_avg(horses, 'speed'):.2f} km/h")

print(f"Slowest horse speed: {aggregate_min(horses, 'speed')} km/h")
print(f"Fastest horse speed: {aggregate_max(horses, 'speed')} km/h")
print()

def print_speed_table(table, title):
    print(f"** {title} **")
    print("-" * 30)
    for row in table[1:]:
        print(f"{row[1]}: {row[3]} km/h")
    print("-" * 30)
    print()

abs_speeds = scalar_abs(horses, "speed")
sqrt_speeds = scalar_sqrt(horses, "speed")
pow_speeds = scalar_pow(horses, "speed", 2)

print_speed_table(abs_speeds, "Absolute Speeds: ")
print_speed_table(sqrt_speeds, "Square Roots of Speeds: ")
print_speed_table(pow_speeds, "Speeds Squared: ")
