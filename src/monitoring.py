

def count_log_levels(log_file_path):
    """Counts the number of INFO, WARNING, and ERROR logs in a log file."""
    counts = {"INFO": 0, "WARNING": 0, "ERROR": 0, "CRITICAL": 0}

    with open(log_file_path, "r") as log_file:
        for line in log_file:
            if "INFO" in line:
                counts["INFO"] += 1
            elif "WARNING" in line:
                counts["WARNING"] += 1
            elif "ERROR" in line:
                counts["ERROR"] += 1
            elif "CRITICAL" in line:
                counts["CRITICAL"] +=1

    return counts
