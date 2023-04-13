--V_AGG_HOURLY_TRANSACTIONS_BY_TYPE: Fetch the aggregate of the
--transactions started within the last hour from the 9PM of 16th March 2023 across all the accounts.
--The returned set should be grouped by their type
--CREATE VIEW V_AGG_HOURLY_TRANSACTIONS_BY_TYPE AS
CREATE OR REPLACE VIEW V_AGG_HOURLY_TRANSACTIONS_BY_TYPE_16th_March AS
    SELECT ty.transaction_type, COUNT(tr.transaction_id) AS num_transactions, SUM(tr.amount) AS total_amount
    FROM transaction_table tr
             JOIN transaction_type ty ON tr.transaction_type = ty.transaction_type_id
    WHERE TRUNC(tr.time_stamp, 'HH24') = TO_DATE('16-MAR-2023 21:00:00', 'DD-MON-YYYY HH24:MI:SS') - INTERVAL '1' HOUR
      AND tr.Status != 'Failed'
    GROUP BY ty.transaction_type
    ORDER BY ty.transaction_type;

--V_AGG_HOURLY_TRANSACTIONS_BY_TYPE: Fetch the aggregate of the
--transactions started within the last hour from the current timestamp across all the accounts.
--The returned set should be grouped by their type
--CREATE VIEW V_AGG_HOURLY_TRANSACTIONS_BY_TYPE AS
CREATE OR REPLACE VIEW V_AGG_HOURLY_TRANSACTIONS_BY_TYPE AS
SELECT ty.transaction_type, COUNT(tr.transaction_id) AS num_transactions, SUM(tr.amount) AS total_amount
FROM transaction_table tr
         JOIN transaction_type ty ON tr.transaction_type = ty.transaction_type_id
WHERE Tr.time_stamp BETWEEN SYSDATE - INTERVAL '1' HOUR AND SYSDATE - INTERVAL '1' SECOND
  AND Tr.Status != 'Failed'
GROUP BY ty.transaction_type
ORDER BY ty.transaction_type;

--V_HOURLY_FAILED_TRANSACTION: Fetches all the transactions started in the last
--hour  9PM of 16th March 2023 , which is in a "failure” status.
CREATE OR REPLACE VIEW V_HOURLY_FAILED_TRANSACTION_16th_March AS
    SELECT ty.transaction_type, COUNT(tr.transaction_id) AS num_transactions, SUM(tr.amount) AS total_amount
    FROM transaction_table tr
             JOIN transaction_type ty ON tr.transaction_type = ty.transaction_type_id
    WHERE TRUNC(tr.time_stamp, 'HH24') = TO_DATE('16-MAR-2023 21:00:00', 'DD-MON-YYYY HH24:MI:SS') - INTERVAL '1' HOUR
      AND tr.Status = 'Failed'
    GROUP BY ty.transaction_type
    ORDER BY ty.transaction_type;

--V_HOURLY_FAILED_TRANSACTION: Fetches all the transactions started in the last
--hour  from the current timestamp , which is in a "failure” status.
--CREATE VIEW V_HOURLY_FAILED_TRANSACTION AS
CREATE OR REPLACE VIEW V_HOURLY_FAILED_TRANSACTION AS
    SELECT ty.transaction_type, COUNT(tr.transaction_id) AS num_transactions, SUM(tr.amount) AS total_amount
    FROM transaction_table tr
             JOIN transaction_type ty ON tr.transaction_type = ty.transaction_type_id
    WHERE Tr.time_stamp BETWEEN SYSDATE - INTERVAL '1' HOUR AND SYSDATE - INTERVAL '1' SECOND
      AND Tr.Status = 'Failed'
    GROUP BY ty.transaction_type
    ORDER BY ty.transaction_type;

--V_AGG_DAILY_TRANSACTIONS_BY_TYPE: Fetch the aggregate of the transactions
--started within the last day from 16th March 2023 across all the accounts. The returned
--set should be grouped by their type
CREATE OR REPLACE VIEW V_AGG_DAILY_TRANSACTIONS_BY_TYPE_15th_March AS
    SELECT tr.transaction_type, COUNT(T.transaction_id) AS num_transactions, SUM(T.amount) as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
    WHERE TRUNC(T.time_stamp) = TRUNC(TO_DATE('16-MAR-2023', 'DD-MON-YYYY')) - 1
      AND T.Status != 'Failed'
    GROUP BY tr.transaction_type
    ORDER BY tr.transaction_type;

--V_AGG_DAILY_TRANSACTIONS_BY_TYPE: Fetch the aggregate of the transactions
--started within the last day from current date across all the accounts. The returned
--set should be grouped by their type
CREATE OR REPLACE VIEW V_AGG_DAILY_TRANSACTIONS_BY_TYPE As
    SELECT tr.transaction_type, COUNT(T.transaction_id) AS num_transactions, SUM(T.amount) as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
    WHERE TRUNC(T.time_stamp) = TRUNC(SYSDATE) - 1
      AND T.Status != 'Failed'
    GROUP BY tr.transaction_type
    ORDER BY tr.transaction_type;

--V_DAILY_FAILED_TRANSACTIONS: Fetches all the transactions started in the last
--day from 16th March 2023, which is in a "failure” status.
CREATE OR REPLACE VIEW V_DAILY_FAILED_TRANSACTIONS_15th_March AS
    SELECT tr.transaction_type, COUNT(T.transaction_id) AS num_transactions, SUM(T.amount) as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
    WHERE TRUNC(T.time_stamp) = TRUNC(TO_DATE('16-MAR-2023', 'DD-MON-YYYY')) - 1
      AND T.Status = 'Failed'
    GROUP BY tr.transaction_type
    ORDER BY tr.transaction_type;

--V_DAILY_FAILED_TRANSACTIONS: Fetches all the transactions started in the last
--day from current date, which is in a "failure” status.
CREATE OR REPLACE VIEW V_DAILY_FAILED_TRANSACTIONS AS
    SELECT tr.transaction_type, COUNT(T.transaction_id) AS num_transactions, SUM(T.amount) as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
    WHERE TRUNC(T.time_stamp) = TRUNC(SYSDATE) - 1
      AND T.Status = 'Failed'
    GROUP BY tr.transaction_type
    ORDER BY tr.transaction_type;

--V_AGG_DAILY_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last day from 16th March 2023 across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_DAILY_TRANSACTIONS_BY_BRANCH_15th_March AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE TRUNC(T.time_stamp) = TRUNC(TO_DATE('16-MAR-2023', 'DD-MON-YYYY')) - 1
      AND T.Status != 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_DAILY_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last day from current date across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_DAILY_TRANSACTIONS_BY_BRANCH AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE TRUNC(T.time_stamp) = TRUNC(SYSDATE) - 2
      AND T.Status != 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_DAILY_FAILED_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last day from 16th March 2023 across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_DAILY_FAILED_TRANSACTIONS_BY_BRANCH_15th_March AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE TRUNC(T.time_stamp) = TRUNC(TO_DATE('16-MAR-2023', 'DD-MON-YYYY')) - 1
      AND T.Status = 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_DAILY_FAILED_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last day from current date across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_DAILY_FAILED_TRANSACTIONS_BY_BRANCH AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE TRUNC(T.time_stamp) = TRUNC(SYSDATE) - 1
      AND T.Status = 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_HOURLY_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last hour from 9PM of 16th March 2023 across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_HOURLY_TRANSACTIONS_BY_BRANCH_16th_March AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE TRUNC(T.time_stamp, 'HH24') = TO_DATE('16-MAR-2023 21:00:00', 'DD-MON-YYYY HH24:MI:SS') - INTERVAL '1' HOUR
      AND T.Status != 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_HOURLY_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last hour from current timestamp across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_HOURLY_TRANSACTIONS_BY_BRANCH AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           as total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE T.time_stamp BETWEEN SYSDATE - INTERVAL '1' HOUR AND SYSDATE - INTERVAL '1' SECOND
      AND T.Status != 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_HOURLY_FAILED_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last hour from  9PM of 16th March 2023 across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_HOURLY_FAILED_TRANSACTIONS_BY_BRANCH_16th_March AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           AS total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE TRUNC(T.time_stamp, 'HH24') = TO_DATE('16-MAR-2023 21:00:00', 'DD-MON-YYYY HH24:MI:SS') - INTERVAL '1' HOUR
      AND T.Status = 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_AGG_HOURLY_FAILED_TRANSACTIONS_BY_BRANCH: Fetch the aggregate of the
--transactions started within the last hour from  current timestamp across all the accounts.
--The returned set should be grouped by the branch from where the transactions originated
--from.
CREATE OR REPLACE VIEW V_AGG_HOURLY_FAILED_TRANSACTIONS_BY_BRANCH AS
    SELECT B.branch_name,
           tr.transaction_type,
           COUNT(T.transaction_id) AS num_transactions,
           SUM(T.amount)           AS total_amount
    FROM transaction_table T
             JOIN transaction_type tr ON T.transaction_type = tr.transaction_type_id
             JOIN accounts A ON T.account_id = A.account_id
             JOIN branch B ON A.branch_id = B.branch_id
    WHERE T.time_stamp BETWEEN SYSDATE - INTERVAL '1' HOUR AND SYSDATE - INTERVAL '1' SECOND
      AND T.Status = 'Failed'
    GROUP BY B.branch_name, tr.transaction_type
    ORDER BY B.branch_name, tr.transaction_type;

--V_CUSTOMER_ACCOUNT_BALANCES: Fetches all customer information with their
--account balances and account types, grouped by customer.
--CREATE VIEW V_CUSTOMER_ACCOUNT_BALANCES AS
CREATE OR REPLACE VIEW V_CUSTOMER_ACCOUNT_BALANCES AS
    SELECT c.customer_id, c.first_name, c.last_name, t.account_type, SUM(a.balance) AS total_balance
    FROM customer c
             JOIN accounts a ON c.customer_id = a.customer_id
             JOIN account_type t ON a.account_type = t.account_type_id
    GROUP BY c.customer_id, c.first_name, c.last_name, a.account_type, t.account_type
    ORDER BY c.customer_id, c.first_name, c.last_name;

--V_ACCOUNTS_BRANCH_INFO: Fetches all accounts with their branch information
--including branch name and branch code, grouped by account.
--CREATE VIEW V_ACCOUNTS_BRANCH_INFO AS
CREATE OR REPLACE VIEW V_ACCOUNTS_BRANCH_INFO AS
    SELECT a.account_id,
           a.customer_id,
           c.first_name ||' '|| c.last_name as customer_name,
           t.account_type,
           a.balance,
           b.branch_name,
           b.branch_code
    FROM accounts a
             JOIN branch b ON a.branch_id = b.branch_id
             JOIN customer c ON a.customer_id = c.customer_id
             JOIN account_type t ON a.account_type = t.account_type_id
    GROUP BY a.account_id, a.customer_id, c.first_name, c.last_name, t.account_type, a.balance, b.branch_name, b.branch_code
    ORDER BY a.account_id, a.customer_id;

--V_TRANSACTION_DETAILS: Fetches all transaction information with details including
--transaction ID, customer account number, transaction amount, account type, and
--transaction type, grouped by customer.
CREATE OR REPLACE VIEW V_TRANSACTION_DETAILS AS
SELECT t.transaction_id, t.account_id, a.customer_id, c.first_name || ' ' || c.last_name AS customer_name, t.amount, ty.account_type, t.time_stamp, tr.transaction_type, t.status
FROM transaction_table t
JOIN accounts a ON t.account_id = a.account_id
JOIN account_type ty ON a.account_type = ty.account_type_id
JOIN transaction_type tr ON t.transaction_type = tr.transaction_type_id
JOIN customer c on a.customer_id = c.customer_id
GROUP BY a.customer_id, t.account_id, t.transaction_id, c.first_name || ' ' || c.last_name, t.amount, ty.account_type, t.time_stamp, tr.transaction_type, t.status
ORDER BY t.transaction_id;

--V_LOANS_CUSTOMER_INFO: Fetches all loans with customer email, address, and
--customer information including customer name, branch name, branch code, and loan type
--descriptions, grouped by customer.
CREATE OR REPLACE VIEW V_LOANS_CUSTOMER_INFO AS
SELECT l.loan_id, c.first_name || ' ' || c.last_name AS customer_name, c.email_id, c.address,  b.branch_name, b.branch_code, lt.loan_type_id as loan_type, lt.loan_description AS loan_type_description
FROM loan l
JOIN customer c ON l.customer_id = c.customer_id
JOIN accounts a ON c.customer_id = a.customer_id
JOIN branch b ON a.branch_id = b.branch_id
JOIN loan_type lt ON l.loan_type = lt.loan_type_id
GROUP BY l.loan_id, c.email_id, c.address, c.first_name || ' ' || c.last_name, b.branch_name, b.branch_code, lt.loan_type_id, lt.loan_description
ORDER BY l.loan_id, c.email_id;

--V_EMPLOYEE_POSITION_INFO: Fetches all employees with their position
--information, branch name, and manager name, as well as associated manager's email
--address and phone number, grouped by employee.
CREATE OR REPLACE VIEW V_EMPLOYEE_POSITION_INFO AS
SELECT e.employee_id, e.first_name || ' ' || e.last_name AS employee_name, e.email_id, e.phone_number, r.position_name, b.branch_name, m.first_name || ' ' || m.last_name AS manager_name, m.email_id AS manager_email, m.phone_number AS manager_phone
FROM employee e
JOIN role_table r ON e.role_id = r.role_id
JOIN branch b ON e.branch_id = b.branch_id
LEFT JOIN employee m ON e.manager_id = m.employee_id
GROUP BY e.employee_id, e.first_name, e.last_name, e.email_id, e.phone_number, r.position_name, b.branch_name, m.first_name, m.last_name, m.email_id, m.phone_number
ORDER BY e.employee_id, e.first_name;