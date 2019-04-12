import re
import time

from src.constants import *


# def print_per_message(per_func):
#     print "in wrapper"
#     def wrapper_func(self, **kwargs):
#         message = kwargs.get("message")
#         query = kwargs.get("query")
#
#         start = time.time()
#
#         if query:
#             per_func(query, self.company_id)
#         else:
#             per_func(self)
#
#         print PRINT_FORMAT.format(message=message, time=time.time() - start)
#
#     return wrapper_func


def print_message(message):
    def wrapper_func(func):
        def wrapped_func(self):
            start = time.time()
            func(self)
            print PRINT_FORMAT.format(message=message, time=time.time() - start)

        return wrapped_func

    return wrapper_func


def print_message(function, message):
    start = time.time()
    function()
    print PRINT_FORMAT.format(message=message, time=time.time() - start)


def parse_exp(exp):
    group_data = list()
    parsed_exp = re.findall(FETCH_REV_DP_REGEX, exp)
    key = ''

    for each_exp in parsed_exp:
        key = each_exp[0]
        group_data.append(int(each_exp[1]))

    return key, group_data
