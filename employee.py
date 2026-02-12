import json

class Employee:
    def __init__(self, emp_id: int, first_name: str, last_name: str, dob: str, city: str, salary: int, action: str):
        # self.action_id = action_id
        self.emp_id = emp_id
        self.first_name = first_name
        self.last_name = last_name
        self.dob = dob
        self.city = city
        self.salary = salary
        self.action = action
        
    @staticmethod
    def from_line(line):
        # return Employee(line[0],line[1],line[2],line[3],str(line[4]),line[5],line[6], line[7])
        return Employee(line[0],line[1],line[2],str(line[3]),line[4],line[5], line[6])
    

    def to_json(self):
        return json.dumps(self.__dict__, default=str)
