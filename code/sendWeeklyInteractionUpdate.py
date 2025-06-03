from reportMarkingFunctions import *
from datetime import date, timedelta


def main():
    # Get today's date
    today = date.today()
    lastWeek = (today - timedelta(days=7)).isoformat()
    last4Weeks = (today - timedelta(days=28)).isoformat()

    # Print a nice header
    print("The interaction report is below:\n")

    # Print the interaction report for the last week
    print("Since", lastWeek)
    get_grade_counts_since(lastWeek)

    # Print the interaction report for the last 4 weeks
    print("\nSince", last4Weeks)
    get_grade_counts_since(last4Weeks)


if __name__ == "__main__":
    main()
