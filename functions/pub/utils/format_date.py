from datetime import datetime


def convert_date_format(date_str: str) -> str:
    """
    Converts a date from 'YYYY-MM-DD' to 'DD/MM/YYYY' format.
    
    Args:
        date_str (str): Date string in 'YYYY-MM-DD' format.
    
    Returns:
        str: Date string in 'DD/MM/YYYY' format.
    
    Raises:
        ValueError: If the input date format is invalid.
    """
    try:
        if date_str in [None, ""]:
            return ""

        # Parse the input date string
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        # Format into the desired output
        return date_obj.strftime("%d/%m/%Y")
    except ValueError:
        raise ValueError("Invalid date format. Expected 'YYYY-MM-DD'.")
