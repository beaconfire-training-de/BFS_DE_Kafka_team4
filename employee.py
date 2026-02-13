"""
Employee data model class for CDC (Change Data Capture) operations.

This class represents an employee record with all necessary fields for tracking
changes in the source database and replicating them to the destination database.
"""

import json


class Employee:
    """
    Employee data model representing a single employee record.
    
    Attributes:
        action_id (int): Unique identifier for the CDC action/change
        emp_id (int): Employee ID (primary key)
        emp_FN (str): Employee first name
        emp_LN (str): Employee last name
        emp_dob (str): Date of birth (as string)
        emp_city (str): City where employee is located
        emp_salary (int): Employee salary
        action (str): Type of change - 'INSERT', 'UPDATE', or 'DELETE'
    """
    
    def __init__(self, action_id: int, emp_id: int, emp_FN: str, emp_LN: str, 
                 emp_dob: str, emp_city: str, emp_salary: int, action: str):
        """
        Initialize an Employee object.
        
        Args:
            action_id: Unique identifier for the CDC action
            emp_id: Employee ID
            emp_FN: First name
            emp_LN: Last name
            emp_dob: Date of birth
            emp_city: City
            emp_salary: Salary
            action: Action type ('INSERT', 'UPDATE', or 'DELETE')
        """
        self.action_id = action_id
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
        """
        Create an Employee object from a database record tuple.
        
        Args:
            line: Tuple containing (action_id, emp_id, first_name, last_name, dob, city, salary, action)
            
        Returns:
            Employee: Employee object created from the tuple
        """
        # Parse tuple: (action_id, emp_id, first_name, last_name, dob, city, salary, action)
        return Employee(line[0], line[1], line[2], line[3], str(line[4]), line[5], line[6], line[7])

    def to_json(self):
        """
        Convert Employee object to JSON string.
        Used for serializing employee data when sending to Kafka.
        
        Returns:
            str: JSON string representation of the employee object
        """
        return json.dumps(self.__dict__)
        # return Employee(line[0],line[1],line[2],line[3],str(line[4]),line[5],line[6], line[7])
        return Employee(line[0],line[1],line[2],str(line[3]),line[4],line[5], line[6])
    

    def to_json(self):
        return json.dumps(self.__dict__, default=str)
