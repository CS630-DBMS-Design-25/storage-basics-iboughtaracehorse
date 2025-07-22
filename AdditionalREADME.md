# DBMS Project Part 4: Advanced Query Execution (not so advanced but thats the topic for another day)

This project implements advanced DBMS features as a standalone Python program.  
It focuses on:

- **Merge Join**: Efficiently joins two tables on a specified column.
- **Aggregation Functions**: Computes SUM, AVG, MIN, and MAX on numeric columns.
- **Scalar Functions**: Applies numeric transformations (ABS, SQRT, POW) on entire columns.

## Tables Used

- **horses**: Represents horses with attributes: `id`, `name`, `stable_id`, and `speed`.
- **stables**: Represents stables with attributes: `stable_id` and `stable_name`.

## Features

- **Merge Join**: Join `horses` with `stables` on `stable_id` to associate each horse with its stable.
- **Aggregation**: Compute total, average, minimum, and maximum speeds of horses.
- **Scalar Functions**: Apply mathematical operations (absolute value, square root, power) on horse speeds.

## Usage

1. Run the script:

   ```bash
   python your_script.py
