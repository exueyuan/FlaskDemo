import time
import math


def formate_time(time_str):
    """
    :param time_str: 时间字符串，"2021-09-30 12:31:28"
    :return:
    """
    if not time_str:
        time_tuple = time.localtime(time.time())
    else:
        time_tuple = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    season = math.ceil(time_tuple.tm_mon / 3)
    weekofyear = int(time.strftime("%U", time_tuple))
    # 需要返回"hour", "month","weekofyear", "season", "dayofweek"
    return time_tuple.tm_hour, time_tuple.tm_mon, weekofyear, season, time_tuple.tm_wday + 1


print(formate_time("2021-5-1 8:29:30"))
# print(formate_time(""))