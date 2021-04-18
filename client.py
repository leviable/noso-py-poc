from collections import defaultdict
from hashlib import sha256
import multiprocessing as mp
import queue
from pprint import pformat as pf
import socket
import sys
import time

MINER_VERSION = '1.6Levi01'

JOINOK = "JOINOK"
PASSFAILED = "PASSFAILED"
PONG = "PONG"
POOLSTEPS = "POOLSTEPS"
STEPOK = "STEPOK"

r_num_map = dict()
for x in range(100000):
    r_num_map[x] = str(x).rjust(5, '0').encode()

l_num_map = dict()
for x in range(1000, 10000):
    l_num_map[x] = str(x).encode()


class NosoClientError(Exception):
    pass


class Client:
    def __init__(self, ip, port, password, noso_addr):
        self.addr = (ip, port)
        self.pw = password
        self.noso_addr = noso_addr
        self.auth = f"{self.pw} {self.noso_addr}"

        self.sock = None
        self.hash_rate = 0
        self.miner_steps = 0  # Step solutions found by miner
        self._target_block = 0
        self._target_string = ''
        self._target_chars = 0
        self._miner_seed = '' 
        self._pool_addr = ''
        self._found_steps = 0
        self._balance = 0.0
        self._blocks_till_payment = 1000
        self._block_difficulty = 1000
        self._pool_hashrate = 0

    def _send(self, msg, with_recv=True):
        msg = " ".join([self.auth, msg]) if msg else self.auth
        print("***************")
        print(f"Sending: {msg}")
        print("***************")
        self.sock.send(f"{msg}\n".encode())
        if with_recv:
            self._recv()

    def _recv(self):
        resp_raw = self.sock.recv(4096).decode().strip()
        print("***************")
        print("Received:")
        print(resp_raw)
        print("***************")
        for resp in parse_resp(resp_raw):
            if resp.code == JOINOK:
                self._pool_addr = resp.pool_addr
                self._miner_seed = resp.miner_seed
                self._target_block = resp.target_block
                self._target_string = resp.target_string
                self._target_chars = resp.target_chars
                self._found_steps = resp.found_steps
                self._block_difficulty = resp.block_difficulty
                self._balance = resp.balance
                self._blocks_till_payment = resp.blocks_till_payment
                self._pool_hashrate = resp.pool_hashrate
            elif resp.code in [PONG, POOLSTEPS]:
                self._target_block = resp.target_block
                self._target_string = resp.target_string
                self._target_chars = resp.target_chars
                self._found_steps = resp.found_steps
                self._block_difficulty = resp.block_difficulty
                self._balance = resp.balance
                self._blocks_till_payment = resp.blocks_till_payment
                self._pool_hashrate = resp.pool_hashrate
            elif resp.code == STEPOK:
                self.miner_steps += 1
            else:
                raise NosoClientError(str(resp))

    def connect(self):
        print(f"Creating socket to: {self.addr}")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.connect(self.addr)
        print("Created")

    def disconnect(self):
        pass

    def join(self):
        # Example resp
        # JOINOK NQ9HnWnQibdP4DdcMwRxeiJJW7orEr !#!!!!!!! PoolData 3392 B58B9F9FCFEDC1B000B3C06B828E6097 10 0 93 0 -98 1098
        self._send(f"JOIN {MINER_VERSION}")

    def ping(self):
        resp = self._send(f"PING {int(self.hash_rate/1000000)}")
    
    def send_step(self, seed, hash):
        resp = self._send(f"STEP {self._target_block} {seed} {hash}", False)

    def run(self):
        work_queue = mp.Queue(100)
        solutions = mp.Queue()
        hash_reports = mp.Queue()

        hash_rates = defaultdict(list)

        miners = list()
        cores = mp.cpu_count()*3//4
        print(f"Using {cores} cores")
        for i in range(cores):
            miners.append(mp.Process(target=miner, args=(i, work_queue, solutions, hash_reports)))
            miners[i].start()

        current_step = self._found_steps
        current_block = self._target_block
        seed_left = self._miner_seed[:-4]
        seed_right = 0
        left_num = 1000
        ping_time = time.time()

        while True:
            # Flush queue and repopulate if block/step changed
            # Reset the seed if block + seed has changed
            if f"{current_block}{current_step}" != f"{self._target_block}{self._found_steps}":
                seed_right = 0
                left_num = 1000
                current_step = self._found_steps
                current_block = self._target_block
                while not work_queue.empty():
                    try:
                        work_queue.get(block=False)
                    except queue.Empty:
                        pass

            # Check for a solution:
            while not solutions.empty():
                sol = solutions.get()
                self.send_step(sol.seed, sol.hash)

            # Fill queue if empty
            while work_queue.empty():
                left_num += 1
                if left_num > 9999:
                    left_num = 1000
                    seed_right += 1

                new_batch = Batch(
                    target=self._target_string[:int(self._target_chars)-0].lower(),
                    seed=f"{seed_left}{str(seed_right).rjust(4, '0')}",
                    pool_addr=self._pool_addr,
                    left_num=left_num,
                    start=0,
                    stop=100_000
                )

                work_queue.put(new_batch)

            # Record hash rates
            while not hash_reports.empty():
                miner_num, hash_rate = hash_reports.get()
                if len(hash_rates[miner_num]) > 100:
                    hash_rates[miner_num].pop(0)
                hash_rates[miner_num].append(hash_rate)


            # See if we need to ping the pool
            if time.time() > ping_time + 5:
                ping_time = time.time()
                seed_str = f"{seed_left}{str(seed_right).rjust(4, '0')}"
                num_str = f"{str(left_num).rjust(4, '0')}0000"
                self.hash_rate = 0
                for k, v in hash_rates.items():
                    self.hash_rate += sum(v)/len(v)
                print(f"Seed: {seed_str}  Num: {num_str} HashRate: {int(self.hash_rate)}")
                self.ping()


def miner(worker_num, work_queue, solutions, hash_reports):
    while True:
        batch = work_queue.get()
        s = sha256(batch.seed+batch.pool_addr+batch.left_num)
        begin = time.time()
        for num in range(batch.start, batch.stop):
            sc = s.copy()
            sc.update(r_num_map[num])
            # sc = sha256(batch.seed+batch.pool_addr+batch.left_num+r_num_map[num])
            solution = sc.hexdigest()
            # if solution.startswith(batch.target):
            if batch.target in solution:
                solutions.put(Solution(batch.seed, batch.left_num, r_num_map[num]))
        end = time.time()
        hash_reports.put((worker_num, int(batch.stop/(end-begin))))


class Batch:
    def __init__(self, target, seed, pool_addr, left_num, start, stop):
        self.target = target.lower()
        self.seed = seed.encode()
        self.pool_addr = pool_addr.encode()
        self.left_num = l_num_map[left_num]
        self.start = start
        self.stop = stop


class Solution:
    def __init__(self, seed, left_num, right_num):
        self.seed = seed.decode()
        self.hash = f"{left_num.decode()}{right_num.decode()}"


class PoolResponseBase:
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return str(f"{pf(self.__dict__)}")

    def __repr__(self):
        return str(self)


class JoinOk(PoolResponseBase):
    def __init__(self, *args):
        # Example
        # JOINOK NQ9HnWnQibdP4DdcMwRxeiJJW7orEr !#!!!!!!! PoolData 3391 76B83459261252E95CE2C203A8F67015 10 0 94 0 -99 1086
        super().__init__(args[0])
        self.pool_addr = args[1]
        self.miner_seed = args[2]
        self.pool_hash_rate = args[3]
        self.target_block = args[4]
        self.target_string = args[5]
        self.target_chars = args[6]
        self.found_steps = args[7]
        self.block_difficulty = args[8]
        self.balance = args[9]
        self.blocks_till_payment = args[10]
        self.pool_hashrate = args[11]


class PoolPong(PoolResponseBase):
    def __init__(self, *args):
        # Example
        # PONG PoolData 3391 76B83459261252E95CE2C203A8F67015 10 0 94 0 -99 1108
        super().__init__(args[0])
        self.target_block = args[2]
        self.target_string = args[3]
        self.target_chars = args[4]
        self.found_steps = args[5]
        self.block_difficulty = args[6]
        self.balance = args[7]
        self.blocks_till_payment = args[8]
        self.pool_hashrate = args[9]


class PoolSteps(PoolResponseBase):
    def __init__(self, *args):
        # Example
        # POOLSTEPS 0
        super().__init__(args[0])
        self.target_block = args[2]
        self.target_string = args[3]
        self.target_chars = args[4]
        self.found_steps = args[5]
        self.block_difficulty = args[6]
        self.balance = args[7]
        self.blocks_till_payment = args[8]
        self.pool_hashrate = args[9]


class StepOk(PoolResponseBase):
    def __init__(self, *args):
        # Example
        # ?
        super().__init__(args[0])


class UnknownMsg(PoolResponseBase):
    def __init__(self, *args):
        super().__init__(args[0])
        self.args = args


resp_types = {
        JOINOK: JoinOk,
        PASSFAILED: PoolResponseBase,
        POOLSTEPS: PoolSteps,
        PONG: PoolPong,
        STEPOK: StepOk
    }


def parse_resp(resp_str):
    resps = list()
    for r in resp_str.split('\n'):
        args = resp_str.split(' ')
        func = resp_types.get(args[0], UnknownMsg)
        resps.append(func(*args))
    return resps


def main():
    ip = "192.168.254.153"
    port = 8083
    password = "LeviPool"
    noso_addr = "N3nCJEtfWSB77HHv2tFdKGL7onevyDg"
    cpu_cnt = 6

    c = Client(ip, port, password, noso_addr)
    print(c.connect())
    print(c.join())
    print(c.ping())
    c.run()


if __name__ == "__main__":
    main()
