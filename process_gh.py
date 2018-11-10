import csv, urllib, json, datetime, sys, time, Queue
from threading import Thread

class DownloadWorker(Thread):
   def __init__(self, queue):
       Thread.__init__(self)
       self.queue = queue

   def run(self):
       while True:
           row, write_file, curDate = self.queue.get()
           process_data(row, write_file, curDate)
           self.queue.task_done()

def process_data(row, write_file, curDate):
    try:
        msisdn = row[1]
        url = "http://xx.xx.xx.xx:xx/xx&customer=%s" % (msisdn)
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        if data["count"] == 0 and data["status"] == "success":
            print "There is no customer id for this msisdn = %s \n" % (msisdn)
            return
        elif data["status"] == "failed":
            print "Error receive customer id from api db for this msisdn %s \n" % (msisdn)
            return
        cus_id = data["results"][0]["IDENTITY_ID"]
        if row[0].lower() == "insert":
            write_file.write("Insert into XXX (YYY,ZZZ,AAA) values ('%s','%s',to_date('%s','DD-MON-RR')); \n" % (cus_id, row[2], curDate))
        elif row[0].lower() == "remove":
            write_file.write("delete from XXX where segment_id='%s' and customer_id='%s'\n" % (row[2], cus_id))
    except Exception as e:
        try:
            print "Exception. Error processing this msisdn = %s \n" % (msisdn)
        except Exception as f:
            print "Exception. Error while processing while read file"
            pass
        return

def main():
    ts = time.time()
    inputFileName = sys.argv[1]
    with open(inputFileName, 'rb') as csvfile:
        print "================Python started processing file %s ============\n" % (inputFileName)
        outputFileName = sys.argv[2]
        spamreader = csv.reader(csvfile, delimiter='|', quotechar='|')
        write_file = open(outputFileName,"w+")
        today = datetime.date.today()
        curDate = today.strftime('%d-%b-%y')

        queue = Queue.Queue()
        for x in range(8):
            worker = DownloadWorker(queue)
            worker.daemon = True
            worker.start()
        for row in spamreader:
            queue.put((row, write_file, curDate))

        queue.join()
        write_file.close()
        print('=============Processing time for %s {} seconds. write to file %s================= \n'.format(time.time() - ts) % (inputFileName,outputFileName))

main()
