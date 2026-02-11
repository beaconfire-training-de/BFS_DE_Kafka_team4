import json

class Employee:
    def __init__(self,  action_id: int, emp_id: int, emp_FN: str, emp_LN: str, emp_dob: str, emp_city: str, emp_salary: int, action: str):
        self.action_id = action_id
        self.emp_id = emp_id
        self.emp_FN = emp_FN
        self.emp_LN = emp_LN
        self.emp_dob = emp_dob
        self.emp_city = emp_city
        self.emp_salary = emp_salary
        self.action = action
        
    @staticmethod
    def from_line(line):
        return Employee(line[0],line[1],line[2],line[3],str(line[4]),line[5],line[6], line[7])

    def to_json(self):
        return json.dumps(self.__dict__)
