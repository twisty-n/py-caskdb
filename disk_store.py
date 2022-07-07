"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""
import os.path
import time
import typing
from dataclasses import dataclass

import format
from format import encode_kv, decode_kv, decode_header


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf

@dataclass
class Entry:
    file_name: str
    value_size: int
    value_position: int
    timestamp: int


END_OF_FILE_POSITION = 2
START_OF_FILE_POSITION = 0


class DiskStorage:
    """
    Implements the KV store on the disk. Single threaded support only. Only one instance of
    this class should be used in a program

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db"):
        self.file_name = file_name
        self.key_dir: typing.Dict[str, Entry] = {}
        self.read_file = open(file_name, "rb")

        self.__load_file_into_key_dir()

        # use append mode to avoid truncating the file
        self.write_file = open(file_name, "ab")
        self.write_file.seek(0, END_OF_FILE_POSITION)

    def set(self, key: str, value: str) -> None:
        current_time = int(time.time())
        size, payload = format.encode_kv(current_time, key, value)
        self.write_file.seek(0, END_OF_FILE_POSITION)
        self.write_file.write(payload)
        self.write_file.flush()
        value_position = self.write_file.tell() - len(value)
        if key in self.key_dir:
            self.key_dir[key].value_position = value_position
            self.key_dir[key].timestamp = current_time
            self.key_dir[key].value_size = len(value)
        else:
            self.key_dir[key] = Entry(
                file_name=self.file_name,
                timestamp=current_time,
                value_size=len(value),
                value_position=value_position
            )

    def get(self, key: str) -> str:
        if key in self.key_dir:
            entry = self.key_dir[key]
            self.read_file.seek(entry.value_position, START_OF_FILE_POSITION)
            return self.read_file.read(entry.value_size).decode(format.ASCII_ENCODING)

        return ""

    def close(self) -> None:
        self.write_file.flush()
        self.write_file.close()
        self.read_file.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)

    def __load_file_into_key_dir(self):
        # Go to the beginning of the file
        self.read_file.seek(0, 0)
        bytes_read: int = 0
        while True:
            header = self.read_file.read(format.HEADER_SIZE)
            if not header:
                return

            if len(header) != 12:
                raise Exception("Expected 12 byte header but got " + str(len(header)) + " bytes")

            bytes_read += format.HEADER_SIZE

            timestamp, key_size, value_size = decode_header(header)

            key = self.read_file.read(key_size).decode(format.ASCII_ENCODING)
            bytes_read += key_size
            value_position = self.read_file.tell()

            # We don't actually store the file in the keystore
            _ = self.read_file.read(value_size)

            self.key_dir[key] = Entry(
                file_name=self.file_name,
                timestamp=timestamp,
                value_size=value_size,
                value_position=value_position
            )
