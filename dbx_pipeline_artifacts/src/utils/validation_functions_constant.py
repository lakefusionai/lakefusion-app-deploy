# Databricks notebook source
import re
from datetime import datetime


# Regular Expressions
EMAIL_REGEX = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'




def validate_email(input_str) -> bool:
    """
    Validate Email Format
    """
    return bool(re.match(EMAIL_REGEX, input_str))

def check_dob_suspicious(input_str, min_age: int = 0, max_age: int = 130) -> bool:
    """
    Checks if date of birth (DOB) is suspicious.
    
    """
    try:
        # Convert the DOB string to a datetime object
        dob_date = input_str
        today = datetime.today()

        # Check if DOB is in the future
        if dob_date > today:
            return True  # DOB is in the future (suspicious)

        # Calculate the person's age
        age = (today - dob_date).days // 365  # Approximate age in years

        # Check if the age is out of the acceptable range
        if age < min_age or age > max_age:
            return True  # Suspicious if the age is too low or too high

        return False  # DOB is not suspicious
    
    except ValueError:
        # Handle invalid date formats
        print("Invalid date format. Please use 'YYYY-MM-DD'.")
        return True  # Consider invalid format as suspicious
    

def is_string_null_or_empty(input_str):
    """
    Check if the column is None or an empty string.
    """
    return input_str is None or input_str.strip() == ""

def is_contains_numbers(input_str):
    """
    Check if the column contains any numbers.
    """
    return any(char.isdigit() for char in input_str)
