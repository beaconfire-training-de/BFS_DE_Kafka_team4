CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
); 

CREATE TABLE IF NOT EXISTS emp_cdc(
    action_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION log_employee_changes()
RETURNS TRIGGER AS $$
BEGIN 
    IF TG_OP = 'INSERT' THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN 
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS employee_audit_trigger ON employees;
CREATE TRIGGER employee_audit_trigger 
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION log_employee_changes();

CREATE tABLE IF NOT EXISTS cdc_offset(
    id INT PRIMARY KEY DEFAULT 1,
    last_action_id INT DEFAULT 0,
    CHECK (id =1)
);

INSERT INTO cdc_offset (id, last_action_id) VALUES (1,0)
ON CONFLICT (id) DO NOTHING;