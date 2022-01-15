
#!/bin/bash

# Loading Data to Hive 

# using ‘,’ as delimiter and loading with mapper 5

sqoop-import --connect jdbc:mysql://localhost/loan_finance -username hiveuser -password hivepassword \
--table latest_loans -create-hive-table -hive-table loan_finance.latest_loans \ 
-hive-import --fields-terminated-by ',' -m 5


# we used “ --hive-drop-import-delims “ as it removes all delims and uses the default hive delimiter ‘\001’ in place

sqoop-import --connect jdbc:mysql://localhost/loan_finance -username hiveuser \
-password hivepassword --table latest_loans -create-hive-table -hive-table \
loan_finance.latest_loans -hive-import -hive-drop-import-delims -m 5

sqoop-import --connect jdbc:mysql://localhost/loan_finance -username hiveuser -password \
hivepassword --table latest_loans -create-hive-table -hive-table loan_finance.latest_loans \
-hive-import --fields-terminated-by '\001' -m 5

sqoop-import --connect jdbc:mysql://localhost/loan_finance -username hiveuser -password \
hivepassword --table history_loans -create-hive-table -hive-table loan_finance.history_loans \
-hive-import -hive-drop-import-delims -m 5

# The sqoop allow text splitter property has to be set to use split-by on a text/string based column

sqoop-import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect \ 
jdbc:mysql://localhost/loan_finance -username hiveuser -password hivepassword --table \
history_loans -create-hive-table -hive-table loan_finance.history_loans -hive-import \
-hive-drop-import-delims -split-by end_of_period


