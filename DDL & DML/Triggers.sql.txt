-- Create Triggers
CREATE OR REPLACE TRIGGER transaction_success_debit
AFTER INSERT ON transaction_table
FOR EACH ROW
BEGIN

   IF (:new.transaction_type in (2, 4)) AND (:new.status = 'Completed') AND (:new.status_code = '00') THEN
      UPDATE accounts
      SET balance = balance - :new.amount
      WHERE account_id = :new.account_id;  
      
      DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was successful.');
      
   END IF;
END;

CREATE OR REPLACE TRIGGER transaction_success_receive
AFTER INSERT ON transaction_table
FOR EACH ROW 
DECLARE
   transfer_type CHAR(30);
BEGIN
   transfer_type := SUBSTR(:new.transaction_details, 1,13);
   IF (transfer_type = 'Transfer from') AND (:new.status = 'Completed') AND (:new.status_code = '00') AND (:new.transaction_type = 3) THEN
      UPDATE accounts
      SET balance = balance + :new.amount
      WHERE account_id = :new.account_id;
      
      DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was successful.');

   END IF;
END;

CREATE OR REPLACE TRIGGER transaction_success_transfer
AFTER INSERT ON transaction_table
FOR EACH ROW 
DECLARE
   transfer_type CHAR(30);
BEGIN
   transfer_type := SUBSTR(:new.transaction_details, 1,11);
   IF (transfer_type = 'Transfer to') AND (:new.status = 'Completed') AND (:new.status_code = '00') AND (:new.transaction_type = 3) THEN
      UPDATE accounts
      SET balance = balance - :new.amount
      WHERE account_id = :new.account_id;
      
      DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was successful.');
      
   END IF;
END;

CREATE OR REPLACE TRIGGER transaction_success_credit
AFTER INSERT ON transaction_table
FOR EACH ROW
BEGIN
   IF (:new.status = 'Completed') AND (:new.status_code = '00') AND (:new.transaction_type = 1) THEN
      UPDATE accounts
      SET balance = balance + :new.amount
      WHERE account_id = :new.account_id;  
      
      DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was successful.');
      
   END IF;  
END;

CREATE OR REPLACE TRIGGER transaction_failure
AFTER INSERT ON transaction_table
FOR EACH ROW
BEGIN  
   IF (:new.status = 'Failed') THEN
      IF (:new.status_code = '06') THEN
         DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was unsuccessful due to some error.');
      END IF;
   
      IF (:new.status_code = '12') THEN
         DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was unsuccessful as it is an invalid transaction.');
      END IF;
   
      IF (:new.status_code = '13') THEN
         DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was unsuccessful because an invalid amount was entered.');
      END IF;
   
      IF (:new.status_code = '14') THEN
        DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was unsuccessful because of invalid card number.');
      END IF;
   
      IF (:new.status_code = '51') THEN
         DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was unsuccessful because of insufficient funds.');
      END IF;
   
      IF (:new.status_code = '54') THEN
         DBMS_OUTPUT.PUT_LINE('Transaction ' || :new.transaction_id || ' was unsuccessful because of expired card.');
      END IF;
      
   END IF;
END;