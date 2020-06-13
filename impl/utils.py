import colorama
import config
from hashlib import md5
from typing import NamedTuple, Callable, Any, Dict

debug = True
debug_to_file = False
trace = True
testing = True

def debug_log(*args):
    """
    log to screen with CYAN
    log to log.txt
    """
    if debug:
        print(colorama.Fore.CYAN + " ", *args, end= colorama.Style.RESET_ALL +"\n")
        if (debug_to_file):
            print(*args, file=open('log.txt', 'a'))

def debug_test(*args):
    if testing:
        print(*args, file=open('log.txt', 'a'))

def debug_log_raft(*args):
    """
    log to screen with red for raft because they both start with r
    log to log.txt
    """
    if debug:
        print(colorama.Fore.RED+ " ", *args, end= colorama.Style.RESET_ALL +"\n")
        if (debug_to_file):
            print(*args, file=open('log.txt', 'a'))

def trace_log(*args):
    """
    log to screen with CYAN
    """
    if trace:
        print(colorama.Fore.CYAN + " ", *args, end=colorama.Style.RESET_ALL + "\n")

def trace_log_raft(*args):
    """
    log to screen with red
    """
    if trace:
        print(colorama.Fore.RED + " ", *args, end=colorama.Style.RESET_ALL + "\n")


def addr_later(starting_addr:int, key1: int, key2: int):
    """
    Check if key1 is after key2 starting at starting_addr in a chord ring
    """
    return addr_diff(starting_addr, key1, key2) > 0

def addr_diff(starting_addr:int, key1: int, key2: int):
    """
    Get how far away key1 is from key2, starting at starting_addr in a chord ring
    """
    if (key1 >= starting_addr):
        key1_diff = key1 - starting_addr
    else:
        key1_diff = key1 + config.HIGHEST_ADDR - starting_addr

    if (key2 >= starting_addr):
        key2_diff = key2 - starting_addr
    else:
        key2_diff = key2 + config.HIGHEST_ADDR - starting_addr

    return key1_diff - key2_diff

def in_interval(start: int, end: int, addr: int):
    """
    Check if addr is in (start, end)
    """
    if end < start:
        return addr > start or addr < end
    else:
        return addr > start and addr < end

def add_addr(addr: int, val: int):
    """
    Add val to an addr in the chord ring
    """
    return (addr + val) % config.HIGHEST_ADDR

def hash_key(key: str):
    """
    Hash the key
    """
    return int(md5(key.encode()).hexdigest(), 16) % config.HIGHEST_ADDR

class Interval(NamedTuple):
    """
    An interval on the chord ring
    """
    bottom: int
    bottom_closed: bool
    top: int
    top_closed: bool

    def in_interval(self, num: int):
        """
        Check if num is in the interval
        """
        if num == self.bottom:
            if self.bottom_closed:
                return True
            else:
                return False

        elif num == self.top:
            if self.top_closed:
                return True
            else:
                return True

        return in_interval(self.bottom, self.top, num)

    def to_string(self):
        """
        Make the interval easy to read
        """
        s = ''

        if self.bottom_closed:
            s += '['
        else:
            s += '('

        s += str(self.bottom) + ',' + str(self.top)

        if self.top_closed:
            s += ']'
        else:
            s += ')'
        return s

def parse_interval_str(interval_str: str):
    """
    Parse an easy to read interval to an Interval.
    If interval: Interval, then 
        parse_interval_str(interval.to_string) === interval
    """
    assert (interval_str[0] == '[' or interval_str[0] == '(' and interval_str[-1] == ']' or interval_str[-1] == ')')

    if interval_str[0] == '[':
        bottom_closed = True
    else:
        bottom_closed = False

    if interval_str[-1] == ']':
        top_closed = True
    else:
        top_closed = False

    bottom, top = [int(val) for val in interval_str[1:-1].split(',')]
    return Interval(bottom, bottom_closed, top, top_closed)

def opposite_interval(interval: Interval):
    """
    Get the complement interval to `interval` in the chord ring.
    """
    return Interval(interval.top, not interval.top_closed, interval.bottom, not interval.bottom_closed)
