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

# Specifying Dynasties and Index Years
To enhance the precision of the disambiguation process, the program allows users to define the dynasties and index years for the persons with biographies in their data. This is achieved through two settings: batchIDByDY and batchIDByIndexYear.

## batchIDByDY
Description: This setting allows users to specify the dynasties for the persons included in their data. By associating individuals with specific dynasties, the program can more accurately match biographies to the correct historical context and individuals in the CBDB database.

Usage: batchIDByDY is set by calling setupConditionsClass.setupDy. Users need to provide a list of dynasty identifiers corresponding to the individuals in their data. This helps in narrowing down the search and match process to the relevant historical periods.

## batchIDByIndexYear
Description: This setting enables users to define index years for the persons in their data. Index years serve as a temporal reference that can significantly improve the matching accuracy.

Usage: batchIDByIndexYear is set by calling setupConditionsClass.setupIndexYear. Users should provide a list of index years that correspond to the significant events or periods in the lives of the individuals in their data. 

# Adjusting Variant Characters Disambiguation Settings
This program includes two settings that significantly affect the disambiguation process: normalizeNameSetting and normalizeBiogSetting. By default, both settings are set to 0. Adjusting these settings to 1 activates the automatic resolution of 異體字 (variant characters) problems, enhancing the ability to disambiguate data more accurately.

## Effects of Enabling Settings
normalizeNameSetting = 1: When enabled, this setting applies normalization to person names, helping to resolve variations in character usage. This can be particularly useful for matching names that may have been recorded using different characters across various sources.

normalizeBiogSetting = 1: This setting applies normalization to biography texts. Enabling it helps in resolving character variations within biographical texts, improving the match accuracy between biography texts and database entries.

## Impact on Disambiguation Results
Enabling these settings has been proven to significantly enhance the disambiguation process, as evidenced by higher scores in data matching. This improvement is primarily due to the program's enhanced ability to recognize and reconcile character variations, leading to more accurate matches between the input data and the CBDB database.

However, it's important to note that while enabling these settings increases the accuracy of high-score matches, it also results in the inclusion of more records with lower scores. Users should consider this trade-off when adjusting these settings, based on their specific needs for accuracy versus comprehensiveness in the disambiguation results.

# Upper-Level Administrative Unit Matching for Biographical Addresses

## addrBelongsMatchForBiogAddr
Description: This configuration option allows for the matching of person' biographical addresses to their corresponding higher-level administrative divisions within CBDB. For example, if a person's biographical address is 貴池, this feature will not only facilitate matching with persons from 貴池, but will also extend the match to include the persons from with 池州, which is the upper-level administrative unit for 貴池.