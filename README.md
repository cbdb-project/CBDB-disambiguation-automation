# CBDB-disambiguation-automation
Using CBDB data points to match biography texts

# Usage
Please extract the latest.db from [latest.7z](https://github.com/cbdb-project/cbdb_sqlite/blob/master/latest.7z) to the program root directory. **(Step 1)**

The program root directory looks like this:

- latest.db (CBDB SQLite database. Extracted from latest.db)

- input.txt (**Please paste the input data from your spreadsheet here. The separator is \t. Step 2)**

  The schema is
  ```
  {your own personid}{\t}{person name}{\t}{biography}
  ```
  
- CBDB-disambiguation-automation.py (**Main program. Step 3, run it**)

- compare-result-list.csv (Output, text file. **Demo output**)

- compare-result-list.xlsx **(Output, Excel file. Step 4)**

# Dependenies

- sqlite3
  
- pandas

- [char_converter](https://github.com/yukiyuqichen/CHAR)
