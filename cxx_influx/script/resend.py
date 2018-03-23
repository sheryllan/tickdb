import subprocess
import time
import sys
resend_log = '/home/influx/import_qtg/log/resend.log'
resent_cnt = 1
def get_points_cnt(filename):
    cmd = 'grep "book," ' + filename + " | wc -l"
    book_cnt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    cmd = 'grep "trade," ' + filename + " | wc -l"
    trade_cnt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    return int(book_cnt) + int(trade_cnt)

def resend_file(filename):
    global resend_log
    cmd = 'export LD_LIBRARY_PATH=/home/influx/import_qtg/exec:$LD_LIBRARY_PATH\n'
    cmd = cmd + 'export LOG_FILE=' + resend_log + '\n' + '/home/influx/import_qtg/exec/resendfailedmsg ' +  filename + ' ' + sys.argv[2] + ' ' + sys.argv[3] + ' ' + sys.argv[4]
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        print("Failed to resend resend failed points in " + filename)
    else:
        print("resend failed points in " + filename)
def mov_resend_log(filename):
    global resend_log
    mv_cmd = 'mv ' + resend_log + ' ' + filename
    subprocess.call(mv_cmd, shell=True)	
def get_resend_filename():
    return '/home/influx/import_qtg/log/__points' + time.strftime("%d-%m-%YT%H:%M:%S")

resend_file(sys.argv[1])
filename = get_resend_filename()
mov_resend_log(filename)

while get_points_cnt(filename) != 0:
    resend_file(filename)
    filename = get_resend_filename()
    mov_resend_log(filename)
    

print("Finished resend")
