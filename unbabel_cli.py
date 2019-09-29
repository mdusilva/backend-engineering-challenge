import time
import datetime
import json
import queue
import threading
import argparse
import contextlib

@contextlib.contextmanager
def none_context_manager():
    """Dummy context manager, does nothing and returns None"""
    yield None

def streamer(source):
    """
    Generator yielding new lines from a file object

    Loops a file object continuously and yields on each complete line terminating
    in a new line character '\n'. It is assumed that each line is a valid json object, 
    so the output is dictionary.
    
    Parameters
    ----------
    source: file object
        Source file, must have a readline() method

    Yields
    ------
    new_line: dict
        Dictionary corresponding to a line
    """
    new_line = ''
    while True:
        line = source.readline()
        if not line:
            time.sleep(0.001)
        else:
            new_line += line
            if line.endswith('\n'):
                yield json.loads(new_line)
                new_line = ''

def publisher(file_name, pub_queue, window=1, timeout=30):
    """
    Reads lines from a streaming file and puts them in a queue for further processing

    Parameters
    ----------
    file_name: str
        Path of file to be streamed
    pub_queue: queue.Queue object
        Queue to put new lines
    window: int, optional
        Size of censoring window in seconds (default is 1), lines with timestamps
        older than this values will be ignored
    timeout: int, optional
        Timeout for putting items in the queue in seconds (default is 30)
    """
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
    """
    Receives dictionaries from a queue, containing at a minimum a 'timestamp' key, 
    computes a moving average of the value given by the 'duration' key (if it exists) 
    with a given frequency and puts the output in a queue for further processing

    If no 'duration' values are found within a 'window' period, the result will be None

    Parameters
    ----------
    delay: int or float
        frequency of moving average computation in seconds. The moving average is computed every 
        delay seconds. The first computation instant is rounded to the beggining of the next minute if 
        delay is 1 minute or more
    pub_queue: queue.Queue object
        new dictionaries are obtained from this queue
    write_queue: queue.Queue object
        put computed moving average and and respective timestamp in this queue
    window: int or float, optional
        Moving average window in seconds (default is 1)
    """
    next_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    if delay >= 60:
        next_time = next_time.replace(microsecond=0, second=0).timestamp()
    else:
        next_time = next_time.timestamp()
    messages = []
    print("Next time ", datetime.datetime.fromtimestamp(next_time))
    while True:
        sleeping_time = next_time - time.time()
        print ("Sleeping for %s seconds..." % sleeping_time)
        time.sleep(max(0, sleeping_time))
        print("Finished sleeping")
        try:
            now = time.time()
            while not pub_queue.empty():
                messages.append(pub_queue.get(block=False))
            #Where the magic happens: filter all current messages to have only the ones within the window
            #then grab the durations, note that a duration of zero is assumed if the key is not found
            messages = [x for x in messages if now - \
                datetime.datetime.strptime(x.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f').timestamp() < window]
            durations = [x.get('duration') for x in messages if x.get('duration') is not None]
            print("Durations: ", durations)
            if len(durations) > 0:
                average = sum(durations) / len(durations)
            else:
                average = None
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
    """
    Receive dictionaries from a queue and write them to a file or the stdout

    Parameters
    ----------
    file_name: str or None
        Path of output file. If None, the stdout will be used instead
    write_queue: queue.Queue object
        receive dictionaries from this queue  
    """
    with open(file_name, 'w+') if file_name is not None else none_context_manager() as o_file:
        while True:
            try:
                msg = json.loads(write_queue.get())
                print(str(msg), file=o_file, flush=True)
            except Exception as e:
                print("Error writing: %s" % e)

def main(close_event=None):
    """
    Main function: launches the three required threads (publisher, handler and writer)

    Parse required arguments from the command line

    Parameters
    ----------
    close_event: threading.Event object, optional
        Event which can be used to end the process from an outside controlling thread (default is None)
    """
    in_file, out_file, delay, window = parse_arguments()
    pub_queue = queue.Queue()
    write_queue = queue.Queue()
    pub_thread = threading.Thread(target=publisher, args=(in_file, pub_queue), kwargs={'window': window}, daemon=True)
    handler_thread = threading.Thread(target=handler, args=(delay, pub_queue, write_queue), kwargs={'window': window}, daemon=True)
    writer_thread = threading.Thread(target=writer, args=(out_file, write_queue), daemon=True)
    pub_thread.start()
    handler_thread.start()
    writer_thread.start()
    if close_event is None:
        close_event = threading.Event()
    while not close_event.is_set():
        close_event.wait(10)

def parse_arguments():
    """
    Parse arguments from the command line

    Possible arguments are input_file (mandatory), output_file, delay and window

    Returns
    -------
    args.in_file, args.out_file, frequency, window: tuple
        arguments as parsed from the command line or defaults when required
    """
    arg_parser = argparse.ArgumentParser(description="Reads events from a file stream and writes aggregated statistics to a file")
    arg_parser.add_argument('--input_file', required=True, help='Path to the input file', metavar='input file', dest='in_file')
    arg_parser.add_argument('--output_file', default=None, required=False, help='Path to the input file, if not given the output is sent to to stdout [default: None]', metavar='output file', dest='out_file')
    arg_parser.add_argument('--window_size', default=1, required=False, help='Moving average window size in seconds, must be an integer >= 1, the default will be used otherwise [default: 1]', type=int, metavar='Window size', dest='window')
    arg_parser.add_argument('--frequency', default=60, required=False, help='Frequency of moving average calculation in seconds, must be an integer >=1 [default: 60]', type=int, metavar='Frequency', dest='frequency')
    args = arg_parser.parse_args()
    window = args.window if args.window > 0 else arg_parser.get_default('window')
    frequency = args.frequency if args.frequency > 0 else arg_parser.get_default('frequency')
    return args.in_file, args.out_file, frequency, window

if __name__ == "__main__":
    main()