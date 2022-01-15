

-- The IBRD historical and latest loans datasets are being used and insights
-- of datasets are extracted.
-- https://finances.worldbank.org/Loans-and-Credits/IBRD-Statement-of-Loans-Latest-Available-Snapshot/sfv5-tf7p
-- https://finances.worldbank.org/Loans-and-Credits/IBRD-Statement-Of-Loans-Historical-Data/zucq-nrc3


-- Creating a database for our data

create database finance;
use finance;

-- Creating the required tables for loading data

-- Creating a database for historical loans

CREATE TABLE history_loans(
end_of_period datetime,
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
first_repayment_date datetime,
last_repayment_date datetime,
agreement_signing_date datetime,
board_approval_date datetime,
effective_date_most_recent datetime,
closed_date_most_recent datetime,
last_disbursement_date datetime);


-- Creating a database for latest loans

CREATE TABLE latest_loans(
end_of_period datetime,
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
first_repayment_date datetime,
last_repayment_date datetime,
agreement_signing_date datetime,
board_approval_date datetime,
effective_date_most_recent datetime,
closed_date_most_recent datetime,
last_disbursement_date datetime);

-- We loaded data into the file and simultaneously did basic data cleaning by identifying
-- null values, trimming the spaces and also converting the date and time values to
-- acceptable form

LOAD DATA LOCAL INFILE
'/home/ak/project_gladiator/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv'
INTO TABLE history_loans
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14,
@c15, @c16, @c17, @c18, @c19, @c20, @c21, @c22, @c23, @c24, @c25, @c26, @c27,
@c28, @c29, @c30, @c31, @c32, @c33)
SET
loan_number = RTRIM(@c2),
loan_number = LTRIM(@c2),
region = RTRIM(@c3),
region = LTRIM(@c3),
country_code = RTRIM(@c4),
country_code = LTRIM(@c4),
country = RTRIM(@c5),
country = LTRIM(@c5),
borrower = RTRIM(@c6),
borrower = LTRIM(@c6),
guarantor_country_code = RTRIM(@c7),
guarantor_country_code = LTRIM(@c7),
guarantor = RTRIM(@c8),
guarantor = LTRIM(@c8),
loan_type = RTRIM(@c9),
loan_type = LTRIM(@c9),
loan_status = RTRIM(@c10),
loan_status = LTRIM(@c10),
currency_of_commitment = RTRIM(@c12),
currency_of_commitment = LTRIM(@c12),
project_id = RTRIM(@c13),
project_id = LTRIM(@c13),
project_name = RTRIM(@c14),
project_name = LTRIM(@c14),
end_of_period=STR_TO_DATE(@c1,'%c/%d/%Y'),
loan_number=NULLIF(@c2,''),
region=NULLIF(@c3,''),
country_code=NULLIF(@c4,''),
country=NULLIF(@c5,''),
borrower=NULLIF(@c6,''),
guarantor_country_code=NULLIF(@c7,''),
guarantor=NULLIF(@c8,''),
loan_type=NULLIF(@c9,''),
loan_status=NULLIF(@c10,''),
interest_rate=NULLIF(@c11,''),
currency_of_commitment=NULLIF(@c12,''),
project_id=NULLIF(@c13,''),
project_name=NULLIF(@c14,''),
original_principal_amount=NULLIF(@c15,''),
cancelled_amount=NULLIF(@c16,''),
undisbursed_amount=NULLIF(@c17,''),
disbursed_amount=NULLIF(@c18,''),
repaid_to_ibrd=NULLIF(@c19,''),
due_to_ibrd=NULLIF(@c20,''),
exchange_adjustment=NULLIF(@c21,''),
borrowers_obligation=NULLIF(@c22,''),
sold_3rd_party=NULLIF(@c23,''),
repaid_3rd_party=NULLIF(@c24,''),
due_3rd_party=NULLIF(@c25,''),
loans_held=NULLIF(@c26,''),
first_repayment_date=STR_TO_DATE(@c27,'%c/%d/%Y'),
last_repayment_date=STR_TO_DATE(@c28,'%c/%d/%Y'),
agreement_signing_date=STR_TO_DATE(@c29,'%c/%d/%Y'),
board_approval_date=STR_TO_DATE(@c30,'%c/%d/%Y'),
effective_date_most_recent=STR_TO_DATE(@c31,'%c/%d/%Y'),
closed_date_most_recent=STR_TO_DATE(@c32,'%c/%d/%Y'),
last_disbursement_date=STR_TO_DATE(@c33,'%c/%d/%Y');


-- We removed all duplicate rows and created a new table

CREATE TABLE history_loans 
SELECT DISTINCT
end_of_period,
loan_number,region,
country_code,country,
borrower,
guarantor_country_code,
guarantor,
loan_type,
loan_status,
interest_rate,
currency_of_commitment,
project_id,
project_name,
original_principal_amount,
cancelled_amount,
undisbursed_amount,
disbursed_amount,
repaid_to_ibrd,
due_to_ibrd,exchange_adjustment,
borrowers_obligation,
sold_3rd_party,
repaid_3rd_party,
due_3rd_party,
loans_held,
first_repayment_date,
last_repayment_date,
agreement_signing_date,
board_approval_date,
effective_date_most_recent,
closed_date_most_recent,
last_disbursement_date FROM history_loans_2;


-- We added a new column to act as a primary key (monotonically increasing row number)
-- to try primary key based features further on the project workflow

ALTER TABLE history_loans ADD COLUMN serial_no int not null;
set @a:=0;
UPDATE history_loans set serial_no =@a:=@a+1 order by 'rownum';
ALTER TABLE history_loans add constraint primary key(serial_no);


