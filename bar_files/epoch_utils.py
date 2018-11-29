from datetime import datetime

def epoch_to_human_nano_time(epoch):
    dt = datetime.fromtimestamp(epoch // 1000000000)
    s = dt.strftime('%H:%M:%S')
    s += '.' + str(int(epoch % 1000000000)).zfill(9)
    return s
