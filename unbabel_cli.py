import time
import datetime
import json
import queue
import threading
import argparse
import contextlib

@contextlib.contextmanager
def none_context_manager():
    yield None

def streamer(source):
    new_line = ''
    while True:
        # where = in_file.tell()
        line = source.readline()
        if not line:
            # in_file.seek(where)
            time.sleep(0.001)
        else:
            new_line += line
            if line.endswith('\n'):
                yield json.loads(new_line)
                new_line = ''

def publisher(file_name, pub_queue, window=1, timeout=30):
    with open(file_name, 'r') as in_file:
        for l in streamer(in_file):
            try:
                now = time.time()
                dt = l.get('timestamp')
                #Timestamps older than window value are ignored
                if dt is not None and now - datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S.%f').timestamp() < window:
                    print ("Putting", l)
                    pub_queue.put(l, timeout=timeout)
            except Exception as e:
                print("Error publishing: %s" % e)

def handler(delay, pub_queue, write_queue, window=1):
    next_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    if delay >= 60:
        next_time = next_time.replace(microsecond=0, second=0).timestamp()
    messages = []
    print("Next time ", datetime.datetime.fromtimestamp(next_time))
    while True:
        sleeping_time = next_time - time.time()
        print ("Sleeping for %s seconds..." % sleeping_time)
        time.sleep(max(0, sleeping_time))
        print("Finished sleeping")
        try:
            now = time.time()
            # now = datetime.datetime.strptime("2018-12-26 18:24:00", '%Y-%m-%d %H:%M:%S').timestamp()
            while not pub_queue.empty():
                messages.append(pub_queue.get(block=False))
            #Where the magic happens: filter all current messages to have only the ones within the window
            #then grab the durations, note that a duration of zero is assumed if the key is not found
            messages = [x for x in messages if now - \
                datetime.datetime.strptime(x.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f').timestamp() < window]
            durations = [x.get('duration', 0) for x in messages]
            print("Durations: ", durations)
            if len(durations) > 0:
                average = sum(durations) / len(durations)
                msg = json.dumps(
                    {
                        'date': datetime.datetime.fromtimestamp(now).isoformat(' '), 
                        'average_delivery_time': average
                        }
                )
                write_queue.put(msg)
        except Exception as e:
            print("Error handling: %s" % e)
        #next time is adjusted to avoid drifting and to jump multiples of delay if processing took to long
        next_time += (time.time() - next_time) // delay * delay + delay

def writer(file_name, write_queue):
    with open(file_name, 'w+') if file_name is not None else none_context_manager() as o_file:
        while True:
            try:
                msg = json.loads(write_queue.get())
                print(str(msg), file=o_file, flush=True)
            except Exception as e:
                print("Error writing: %s" % e)

def main(in_file, out_file, delay, window):
    pub_queue = queue.Queue()
    write_queue = queue.Queue()
    pub_thread = threading.Thread(target=publisher, args=(in_file, pub_queue), kwargs={'window': window}, daemon=True)
    handler_thread = threading.Thread(target=handler, args=(delay, pub_queue, write_queue), kwargs={'window': window}, daemon=True)
    writer_thread = threading.Thread(target=writer, args=(out_file, write_queue), daemon=True)
    pub_thread.start()
    handler_thread.start()
    writer_thread.start()
    while True:
        time.sleep(10)

if __name__ == "__main__":
    delay = 60  #Hardcoded for now
    arg_parser = argparse.ArgumentParser(description="Reads events from a file stream and writes aggregated statistics to a file")
    arg_parser.add_argument('--input_file', required=True, help='Path to the input file', metavar='input file', dest='in_file')
    arg_parser.add_argument('--output_file', default=None, required=False, help='Path to the input file, if not given the output is sent to to stdout [default: None]', metavar='output file', dest='out_file')
    arg_parser.add_argument('--window_size', default=1, required=False, help='Moving average window size in seconds, must be an integer >= 1, the default will be used otherwise [default: 1]', type=int, metavar='Window size', dest='window')
    args = arg_parser.parse_args()
    window = args.window if args.window > 0 else arg_parser.get_default('window')
    main(args.in_file, args.out_file, delay, window)