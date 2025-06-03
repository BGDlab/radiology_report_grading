import pandas as pd
import numpy as np


def deal_with_next(df, fn, next_step, column, idx):
    """
    Edit a specified column in a dataframe

    Args:
        df (dataframe): table with a user's report grades
        fn (string): Filename to save table to
        next_step (string): What the next step is. 
                            Options: n, p, r, s, e
        column (string): Name of column to edit
        idx (int): Row index

    Return:
        idx (int): Next row index
        end (boolean): If the process should end or continue

    """
    end = False
    print(next_step)
    print(idx)

    if next_step == "n":
        idx = df[df[column].isnull()].index[0]
        print()
    elif next_step == "p":
        # decrement index
        print("Revisiting the previous report...")
        idx -= 1
        print()
    elif next_step == "r":
        print("Repeating the current report...")
        print()
    elif next_step == "s":
        print("Saving and continuing...")
        saved = safely_save_df(df, fn)
        print(saved)
        idx = df[df[column].isnull()].index[0]
        print()
    elif next_step == "e":
        print("Saving and exiting...")
        safely_save_df(df, fn)
        end = True

    return idx, end


def safely_save_df(df, fn):
    """
    Save dataframe only if no permission errors

    Args:
        df (dataframe): Dataframe to save
        fn (string): Filename to save the dataframe under

    Return:
        Boolean if dataframe was able to save safely.
    """
    
    try:
        df = df.astype(str)
        df.to_csv(fn, index=False)
        return True
    except PermissionError:
        print(
            "Error: write access to "
            + fn
            + " denied. Please check that the file is not locked by Datalad."
        )
        return False


def mark_text_color(line, to_mark, color):
    """
    Highlight a string of text

    Args:
        line (string): Text of a radiology report
        to_mark (string): Text to highlight
        color (string): Color to highlight text in

    Return:
        line (string): Input string with specified text highlight in the 
        specified color with bold black text

    """
    # Sort the list of strings to highlight by length (longest to shortest)
    to_mark = sorted(to_mark, key=len)[::-1]

    if color == "green":
        start = "\x1b[5;30;42m"  # green background, bold black text
    elif color == "yellow":
        start = "\x1b[5;30;43m"  # yellow background, bold black text
    elif color == "red":
        start = "\x1b[5;30;41m"  # red background, bold black text
    elif color == "gray" or color == "grey":
        start = "\x1b[5;30;47m"  # gray background, bold black text

    end = "\x1b[0m"

    if line is np.nan:
        return "<No report available.>"

    if type(to_mark) == str:
        line = line.replace(to_mark, start + to_mark + end)

    elif type(to_mark) == list:
        for phrase in to_mark:
            line = line.replace(str(phrase), start + str(phrase).upper() + end)

    else:
        print("Error: the second argument must be either a string or a list of strings")

    return line


def load_df(fn):
    """
    Load a given dataframe, drop 'Unnamed' columns,
    add columns for 'scan_reason' and 'pat_history'

    Args:
        fn (string): Path to the dataframe to load

    Return:
        df (dataframe): Loaded dataframe
    """
    
    # Load the dataframe
    df = pd.read_csv(fn)
    # If these two columns are not in the dataframe, add them
    if "scan_reason" not in list(df):
        df["scan_reason"] = np.nan
    if "pat_history" not in list(df):
        df["pat_history"] = np.nan

    # If there are "Unnamed:" columns in the dataframe, remove them
    to_drop = [col for col in list(df) if "Unnamed:" in col]
    if len(to_drop) > 0:
        df = df.drop(columns=to_drop)

    return df




# Main
if __name__ == "__main__":
    print("Radiology Report Annotation Helper Library v 0.3")
    print("Written and maintained by Ben Jung, PhD (@bencephalon on Github)")
   