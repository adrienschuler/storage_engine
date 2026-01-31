"""
This module implements a Write-Ahead Log (WAL) for crash recovery.
"""
import json
import os

class WriteAheadLog:
    """
    A simple implementation of a Write-Ahead Log.
    """
    def __init__(self, log_path):
        self.log_path = log_path
        self.log_file = open(log_path, 'a+')

    def write(self, operation, key, value):
        """
        Writes an operation to the log.
        """
        log_entry = {'op': operation, 'key': key, 'value': value}
        self.log_file.write(json.dumps(log_entry) + '\n')
        self.log_file.flush()

    def recover(self, btree):
        """
        Recovers the B-tree from the log.
        """
        self.log_file.seek(0)
        for line in self.log_file:
            log_entry = json.loads(line)
            if log_entry['op'] == 'insert':
                btree.insert(log_entry['key'], log_entry['value'], wal=False)

    def close(self):
        """
        Closes the log file.
        """
        self.log_file.close()

    def clear(self):
        """
        Clears the log file.
        """
        self.close()
        os.remove(self.log_path)
        self.log_file = open(self.log_path, 'a+')
