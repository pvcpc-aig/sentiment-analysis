"""
Takes in data from Julian McAuley's "JSON" datasets
and converts them into CSV fragment files with
some fixed number of lines per CSV file.
"""
import os
import sys
import threading as th
import multiprocessing as mp
from pathlib import Path


# Determines whether multithreading should be employed
# to speed up JSON document processing.
#
# This option may hurt performance on single threaded systems.
threading_enable = True

# For multithreaded parsing of the JSON documents,
# we have to optimally distribute the workload across
# available logical processors.
#
# Obviously we rely on the OS to schedule operations for
# us, but they tend to be good at that.
threading_max_threads = mp.cpu_count()

# The maximum number of lines to store in the line buffer
# for processing by multiple threads. 
#
# For an 8 processor system, this is around 8000 lines/thread.
threading_max_line_buffer_size = 2 ** 16

# Value in the range [0, 1] specifying the portion,
# as a percentage, of each dataset to convert to
# CSV.
max_dataset_portion = 0.25


""" multithreading pseudocode
ReadTS
----------------------------
ready: Boolean

@property
def ready(self):
    with self._ready_lock:
        return self._ready

@property
def ready(self, val):
    with self._ready_lock:
        self._ready = val

@property
def done(self):
    with self._done_lock:
        return self._done

@property
def done(self, val):
    with self._done_lock:
        return self._done

read_cv: ConditionVariable(Semaphore)
proc_cv: ConditionVariable(Lock)
meet: Barrier
prepare: Barrier


Read Thread (manager thread)         | Parsing Thread (subordinate thread) |
-------------------------------------|-------------------------------------|
launch subordinate threads           |                                     |
while needs_content:                 |while True:                          |
  fill line buffer                   |  with readts.read_cv:               |
  with readts.read_cv:               |    while !readts.ready:             |
     readts.ready = True             |      readts.read_cv.wait()          |
     readts.read_cv.notify_all()     |  if readts.done:                    |
     readts.ready = False            |    break                            |
  readts.meet.wait()                 |  copy global line buffer            |
  copy and write parsed data         |  parsed local line buffer           |
with readts.read_cv:                 |  write local parsed line buffer     |
  readts.done = True                 |  readts.meet.wait()                 |
  readts.read_cv.notify_all()        |                                     |
                                     |                                     |
                                     |                                     |
                                     |                                     |
"""

class ReadThreadState:

	def __init__(self, thread_count):
		self._read_lk = th.Lock()
		self._done_lk = th.Lock()
		self._ready = False
		self._done = False
		self.read_cv = th.Condition(th.Semaphore(thread_count))
		self.rejoin = th.Barrier(thread_count)
		self.prepare = th.Barrier(thread_count)

	@property
	def ready(self):
		with self._read_lk:
			return self._read
		
	@property
	def ready(self, val):
		with self._read_lk:
			self._read = val

	@property
	def done(self):
		with self._done_lk:
			return self._done
		
	@property
	def done(self, val):
		with self._done_lk:
			self._done = val


class ParsingThread(th.Thread):

	def __init__(self, readts, lnbuf, index, blksz):
		"""
		:param readts
			An object containing the reading thread state. The reading
			thread will manage all subordinate ParsingThreads.

				readts.ready : Boolean - Whether the global line buffer is
										 ready to be read from and parsed.
				
				readts.

		:param lnbuf
			The line buffer created by the raw_to_csv_mp() function
			where the reading thread will fill the data.
		
		:param index
			The index in the lnbuf from which this thread will
			being parsing.
		
		:param blksz
			The number of elements in total this thread shall process
			in the line buffer.
		
		The inteval of lines this thread will process is [index, index + blksz)
		"""
		# local data definition accessed only in this thread
		self._local = th.local()
		self._local.index = index
		self._local.blksz = blksz
		self._local.raw_block = [] * blksz
		self._local.parsed_block = [] * blksz

		# global definitions accessed outside of this thread
		self._global_lnbuf = lnbuf
		self._global_readts = readts
		self._global_parsed_block = [] * blksz

	def run(self):
		readts = self._global_readts
		while True:
			with readts.read_cv:
				while not readts.ready:
					readts.read_cv.wait()
			if readts.done:
				break
			# copy global line buffer
			# parse local line buffer
			# write local parsed buffer
			readts.rejoin.wait()
			readts.prepare.wait()


def raw_to_csv_mp(amz_ds): 
	"""
	The multithreaded implementation of raw_to_csv()
	"""
	# ensure that the threading settings are
	# valid enough to perform efficiently.
	lnbfsz = threading_max_line_buffer_size
	threads = threading_max_threads

	is_pow2 = 0 == (lnbfsz & (lnbfsz - 1))
	is_gtcpus = lnbfsz > threads
	if not (is_pow2 and is_gtcpus):
		raise RuntimeError("Line buffer size must be a power of 2; must be greater than processor count {threads}")

	# setup the initial data
	raw = amz_ds.joinpath("raw")
	json_f = raw.joinpath("json")
	csv_f = raw.joinpath("csv")
	line_buffer = [] * lnbfsz

	# setup the threads, locks, and other concurrent
	# architecture
	readts = ReadThreadState(threads + 1)     # threads + 1 to include this reading/main thread
	thread_block_size = lnbfsz / threads
	thread_group = []
	for i in range(threading_max_threads):
		thread_group = ParsingThread(readts, line_buffer, i * thread_block_size, thread_block_size)
	
	# Check the dataset size
	ds_lines = 0
	with json_f.open(mode="r") as handle:
		for i, l in enumerate(handle):
			ds_lines = i 
	ds_lines += 1 # adjust for 0-based index
	max_lines = int(ds_lines * max_dataset_portion)

	# We first launch all of our processing threads and
	# make them wait until we load in the first chunk of
	# data
	for t in thread_group:
		t.start()

	# Start reading in the raw information, and dispatch
	# work to other threads.
	def _dispatch(done: bool, csv_h):
		with readts.read_cv:
			readts.ready = True
			readts.read_cv.notify_all()
		readts.rejoin.wait()
		readts.ready = False
		readts.done = done
		readts.prepare.wait()

	with csv_f.open(mode="w") as csv_h:
		with json_f.open(mode="r") as json_h:
			i_mod = 0
			for i, ln in enumerate(json_h):
				i_mod = i % lnbfsz
				if i > max_lines:
					break
				line_buffer[i_mod] = ln
				if lnbfsz == i_mod + 1: # we filled out buffer, dispatch work
					_dispatch(False)
			if i_mod > 0:
				while i_mod < max_lines:
					line_buffer[i_mod] = None
					i_mod += 1
				_dispatch(True)
			



def raw_to_csv(amz_ds):
	if threading_enable:
		raw_to_csv_mp(amz_ds)
		return


if __name__ == "__main__":
	if max_dataset_portion <= 0:
		print("max_dataset_portion must be > 0")
		sys.exit(0)

	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			print(f"Converting dataset {str(dataset)}")
			raw_to_csv(dataset)
