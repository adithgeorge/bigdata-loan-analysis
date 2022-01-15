

-- Hive External Table created over s3 data


CREATE EXTERNAL TABLE latest_loans_s3 (
end_of_period varchar(255),
loan_number varchar(9),
region varchar(35),
country_code varchar(2),
country varchar(20),
borrower varchar(50),
guarantor_country_code varchar(2),
guarantor varchar(20),
loan_type varchar(10),
loan_status varchar(20),
interest_rate float,
currency_of_commitment varchar(10),
project_id varchar(8),
project_name varchar(40),
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
first_repayment_date varchar(255),
last_repayment_date varchar(255),
agreement_signing_date varchar(255),
board_approval_date varchar(255),
effective_date_most_recent varchar(255),
closed_date_most_recent varchar(255),
last_disbursement_date varchar(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3n://lti858/Adith/hive_ext/latest_loans_ext/'
TBLPROPERTIES ("skip.header.line.count"="1");


