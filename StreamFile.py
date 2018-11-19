from confluent_kafka import Producer
import argparse, os, time

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

if __name__ == "__main__":
	p = Producer({'bootstrap.servers': 'kafka-master:9092'})
	parser = argparse.ArgumentParser(description="Stream a file.")
	parser.add_argument("filename" , help="Name of the file you want to stream.")
	parser.add_argument("-p", "--path", help="Directory containing your file. Use current directory if ommitted.")
	args = parser.parse_args()
	if args.path != None:
		base = args.path
	else:
		base = os.getcwd()
	fullname = os.path.join(base, args.filename)
	print(fullname)
	if (os.path.isfile(fullname)):
		if os.path.splitext(fullname)[-1] == ".csv":
			f = open(fullname)
			print("The attributes names are: " + next(f))
			while True:
				try:
					p.poll(0)
					time.sleep(1)
					p.produce('rfid-input', next(f).encode('utf-8'), callback=delivery_report)
				except StopIteration:
					f.close()
					f = open(fullname)
					next(f)
					print("Reached the end of file. Going back to the beginning now.")
		else:
			print("I only stream csv file!!!")

	# while True :
	#     # Trigger any available delivery report callbacks from previous produce() calls
	#     p.poll(0)
	#     count += 1
	#     # Asynchronously produce a message, the delivery report callback
	#     # will be triggered from poll() above, or flush() below, when the message has
	#     # been successfully delivered or failed permanently.
	#     try:
	#         p.produce('stream-sim', ("message " + str(count)).encode('utf-8'), callback=delivery_report)
	#     except BufferError:
	#         continue
	# """
	# p.poll(0)
	# p.produce('stream-sim', ("message " + str(count)).encode('utf-8'), callback=delivery_report)
	# """

	# # Wait for any outstanding messages to be delivered and delivery report
	# # callbacks to be triggered.
	# p.flush()
