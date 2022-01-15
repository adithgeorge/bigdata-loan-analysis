
-- Commands to configure hive to enable dynamic partitioning

hive> set hive.exec.dynamic.partition=true;
hive> set hive.exec.dynamic.partition.mode=nonstrict;

-- Command to create table in hive for dynamic partitioning based on country

CREATE TABLE part_finance (end_of_period TIMESTAMP,
loan_number string,
region string,
country_code string,
borrower string,
guarantor_country_code string,
guarantor string,
loan_type string,
loan_status string,
interest_rate float,
currency_of_commitment string,
project_id string,
project_name string,
original_principal_amount double,
cancelled_amount double,
undisbursed_amount double,
disbursed_amount double,
repaid_to_ibrd double,
due_to_ibrd double,
exchange_adjustment double,
borrowers_obligation double,
sold_3rd_party double,
repaid_3rd_party double,
due_3rd_party double,
loans_held double,
first_repayment_date TIMESTAMP,
last_repayment_date TIMESTAMP,
agreement_signing_date TIMESTAMP,
board_approval_date TIMESTAMP,
effective_date_most_recent TIMESTAMP,
closed_date_most_recent TIMESTAMP,
last_disbursement_date TIMESTAMP) PARTITIONED BY (country string);


-- Inserting the data from an existing table for dynamic partitioning based on country

SET hive.exec.max.dynamic.partitions=200;
SET hive.exec.max.dynamic.partitions.pernode=200;

INSERT OVERWRITE TABLE part_finance PARTITION (country) SELECT
end_of_period,loan_number,region,country_code,borrower,guarantor_country_code,guarantor,l
oan_type,loan_status,interest_rate,currency_of_commitment,project_id,project_name,original_p
rincipal_amount,cancelled_amount,undisbursed_amount,disbursed_amount,repaid_to_ibrd,due
_to_ibrd,exchange_adjustment,borrowers_obligation,sold_3rd_party,repaid_3rd_party,due_3rd_
party,loans_held,first_repayment_date,last_repayment_date,agreement_signing_date,board_ap
proval_date,effective_date_most_recent,closed_date_most_recent,last_disbursement_date,
country from latest_table;

-- Command to create table in hive for dynamic partitioning based on region and country in a hierarchical tree structure


CREATE TABLE part_finance2 (end_of_period TIMESTAMP,
loan_number string,
country_code string,
borrower string,
guarantor_country_code string,
guarantor string,
loan_type string,
loan_status string,
interest_rate float,
currency_of_commitment string,
project_id string,
project_name string,
original_principal_amount double,
cancelled_amount double,
undisbursed_amount double,
disbursed_amount double,
repaid_to_ibrd double,
due_to_ibrd double,
exchange_adjustment double,
borrowers_obligation double,
sold_3rd_party double,
repaid_3rd_party double,
due_3rd_party double,
loans_held double,
first_repayment_date TIMESTAMP,
last_repayment_date TIMESTAMP,
agreement_signing_date TIMESTAMP,
board_approval_date TIMESTAMP,
effective_date_most_recent TIMESTAMP,
closed_date_most_recent TIMESTAMP,
last_disbursement_date TIMESTAMP) PARTITIONED BY (region string,country string) ;


-- Inserting the data from an existing table for dynamic partitioning based on region then country


INSERT OVERWRITE TABLE part_finance2 PARTITION (region,country) SELECT
end_of_period,loan_number,country_code,borrower,guarantor_country_code,guarantor,loan_ty
pe,loan_status,interest_rate,currency_of_commitment,project_id,project_name,original_principa
l_amount,cancelled_amount,undisbursed_amount,disbursed_amount,repaid_to_ibrd,due_to_ibr
d,exchange_adjustment,borrowers_obligation,sold_3rd_party,repaid_3rd_party,due_3rd_party,l
oans_held,first_repayment_date,last_repayment_date,agreement_signing_date,board_approval
_date,effective_date_most_recent,closed_date_most_recent,last_disbursement_date,region,
country from latest_table;

-- Command to set hive configurations to enable bucketing

SET hive.enforce.bucketing =true;

-- Creating a table for bucketing clustered by country into 4 buckets

CREATE TABLE bucket_finance (end_of_period TIMESTAMP,
loan_number string,
region string,
country_code string,
country string,
borrower string,
guarantor_country_code string,
guarantor string,
loan_type string,
loan_status string,
interest_rate float,
currency_of_commitment string,
project_id string,
project_name string,
original_principal_amount double,
cancelled_amount double,
undisbursed_amount double,
disbursed_amount double,
repaid_to_ibrd double,
due_to_ibrd double,
exchange_adjustment double,
borrowers_obligation double,
sold_3rd_party double,
repaid_3rd_party double,
due_3rd_party double,
loans_held double,
first_repayment_date TIMESTAMP,
last_repayment_date TIMESTAMP,
agreement_signing_date TIMESTAMP,
board_approval_date TIMESTAMP,
effective_date_most_recent TIMESTAMP,
closed_date_most_recent TIMESTAMP,
last_disbursement_date TIMESTAMP,
serial_no int) CLUSTERED BY (country) into 4 buckets;

-- Inserting data into table from an existing table for bucketing by country to 4 buckets

INSERT OVERWRITE TABLE bucket_finance SELECT * from latest_table;

-- Creating a table for both dynamic partitioning on country and also bucketing into 4
-- buckets clustered by country

CREATE TABLE bucket_finance2 (end_of_period TIMESTAMP,
loan_number string,
country_code string,
country string,
borrower string,
guarantor_country_code string,
guarantor string,
loan_type string,
loan_status string,
interest_rate float,
currency_of_commitment string,
project_id string,
project_name string,
original_principal_amount double,
cancelled_amount double,
undisbursed_amount double,
disbursed_amount double,
repaid_to_ibrd double,
due_to_ibrd double,
exchange_adjustment double,
borrowers_obligation double,
sold_3rd_party double,
repaid_3rd_party double,
due_3rd_party double,
loans_held double,
first_repayment_date TIMESTAMP,
last_repayment_date TIMESTAMP,
agreement_signing_date TIMESTAMP,
board_approval_date TIMESTAMP,
effective_date_most_recent TIMESTAMP,
closed_date_most_recent TIMESTAMP,
last_disbursement_date TIMESTAMP) PARTITIONED BY (region string) CLUSTERED BY
(country) into 4 buckets;


-- Insert command to insert data into table for both bucketing into 4 buckets clustered by
-- country and dynamic partitioning on region

INSERT OVERWRITE TABLE bucket_finance2 PARTITION (region) SELECT
end_of_period,loan_number,country_code,country,borrower,guarantor_country_code,guarantor,
loan_type,loan_status,interest_rate,currency_of_commitment,project_id,project_name,original_
principal_amount,cancelled_amount,undisbursed_amount,disbursed_amount,repaid_to_ibrd,du
e_to_ibrd,exchange_adjustment,borrowers_obligation,sold_3rd_party,repaid_3rd_party,due_3rd
_party,loans_held,first_repayment_date,last_repayment_date,agreement_signing_date,board_a
pproval_date,effective_date_most_recent,closed_date_most_recent,last_disbursement_date,
region from latest_table;

