import sys


MAX_LINES_TO_PRINT = 20


# Assume that several lines need to be printed.
# However, there are cases where instead of a list a simple string needs to be printed.
# In that case, simply print the string.
# If there are less than 20 lines to be printed, simply print them, otherwise the user should interact with the program.
def printInteractive(array):

    length = len(array)

    if isinstance(array, basestring):
        print(array)
        return

    if length <= MAX_LINES_TO_PRINT:
        printArrayLines(array)
        return

    print("There are " + str(length) + " lines. Printing only the first " + str(MAX_LINES_TO_PRINT) + "\n")
    printArrayLines(array)

    currentCount = MAX_LINES_TO_PRINT
    while currentCount < length:
        print("\n")
        if query_yes_no("Do you want to see the next " + str(MAX_LINES_TO_PRINT) + " entries?"):
            printArrayLines(array, currentCount)
            currentCount += MAX_LINES_TO_PRINT
        else:
            return


# Print the specified number of lines from the array ...
def printArrayLines(array, beginning=0, count=MAX_LINES_TO_PRINT):
    for line in array[beginning:beginning + count]:
        print(line)


# Based on https://stackoverflow.com/questions/3041986/apt-command-line-interface-like-yes-no-input
def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
