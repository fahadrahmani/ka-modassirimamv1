from math import floor, ceil
from random import randrange, sample, choices
from datetime import datetime, timedelta

def get_random_nos(n,Min,Max):
    assert Min<n,f"WTF! How can you divide {n} in {Min} numbers all integers."
    assert Max>Min,f"Careful! Your Upper Limit is smaller that Lower Limit"
#     assert Min%10!=0,"Lower Limit is Not a multiple of 10"
#     assert Max%10!=0,"Upper Limit is Not a multiple of 10"
    no_of_terms = floor((floor(n/Min) + ceil(n/Max))/2)
    no_list = [Min]*no_of_terms
    leftover = n - (Min*no_of_terms)
    print(leftover)
    while leftover > 0:
        diff = 10
        if leftover>1000 and Max-1000 > Min:
            diff=1000
        elif leftover>100 and Max-100 > Min:
            diff=100
        elif leftover>50 and Max-50 > Min :
            diff=50
        i = randrange(no_of_terms)
        if no_list[i] < Max-diff:
            no_list[i] = no_list[i] + diff
            leftover = leftover - diff
    return no_list


def get_random_dates(start_date,end_date, length):
    d1 = datetime.strptime(start_date, "%d-%m-%Y")
    d2 = datetime.strptime(end_date, "%d-%m-%Y")
    assert d2>d1, "Start Date cannot be greater that End Date"
    days = (d2-d1).days + 1 
    td=[]
    if days >= length:
        td=sample(range(days),k=length)
    else:
        td=choices(range(days),k=length)
    date_list = [datetime.strftime(d1 + timedelta(days=i),"%d-%m-%Y") for i in td]
    return date_list
