CREATE TABLE IF NOT EXISTS employees(
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

CREATE TABLE IF NOT EXISTS emp_cdc(
    action_id SERIAL PRIMARY KEY,
    emp_id INT,
    emp_FN VARCHAR(100),
    emp_LN VARCHAR(100),
    emp_dob DATE,
    emp_city VARCHAR(100),
    emp_salary INT,
    action VARCHAR(20)
);

-- CDC Function
CREATE OR REPLACE FUNCTION cdc_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO emp_cdc(emp_id, emp_FN, emp_LN, emp_dob, emp_city, emp_salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO emp_cdc(emp_id, emp_FN, emp_LN, emp_dob, emp_city, emp_salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO emp_cdc(emp_id, emp_FN, emp_LN, emp_dob, emp_city, emp_salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS emp_trigger ON employees;

CREATE TRIGGER emp_trigger
AFTER INSERT OR UPDATE OR DELETE
ON employees
FOR EACH ROW
EXECUTE FUNCTION cdc_function();

CREATE TABLE IF NOT EXISTS producer_offset(
    id INT PRIMARY KEY,
    last_action_id INT
);

INSERT INTO producer_offset(id,last_action_id) VALUES (1,0)
ON CONFLICT (id) DO UPDATE SET last_action_id=0;