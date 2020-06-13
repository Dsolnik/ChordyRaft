from utils import trace_log
import os
import csv
import config

class PersistantKeyValueStore:
    """
    A simple, in-memory, Key Value store.
    """

    def __init__(self):
        self.dict = {}
        self.log = "log_file"

        self.log_out = open(self.log+config.FILE_TYPE, "a")
        self.db = "db"

    async def set(self, key, value):
        """
        Set the value for a key
        """
        trace_log("PersistantStorage: setting key ", key, " to value ", value)
        self.dict[key] = value
        #self.log_set(key, value)

    async def get(self, key):
        """
        Get the value for a key
        """
        return self.dict.get(key, None)

    async def get_keys(self):
        """
        Get all the keys
        """
        return self.dict.keys()

    async def add_dict(self, dic):
        """
        Add a dict to the store.
        """
        for key in dic:
            await self.set(key, dic[key])
    
    async def startup(self):
        """
        Startup
        """
        pass


    # Never ended up using anything below here.

    #RECOVERY PROCESS FOR NODE
    def flush(self):
        db_name = self.db + config.FILE_TYPE
        backup_db_name = self.db + "_backup"+config.FILE_TYPE

        #1. BEGIN WRITING CACHE -> DISK
        with open(backup_db_name, 'w') as csvfile:
            for key in self.dict.keys():
                csvfile.write("%s,%s\n" % (str(key), str(self.dict[key])))
        #2. ONCE ALL CACHE->DISK, BACKUP becomes MAIN
        os.rename(backup_db_name, db_name)

        #CLEAR LOG FILE
        self.log_out.seek(0)
        self.log_out.truncate(0)

    def recover(self):
        db_name = self.db + config.FILE_TYPE
        log_name = self.log + config.FILE_TYPE
        if(not os.path.exists(db_name)):
            return False
        self.dict = {}
        try:
            with open(db_name, 'r') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                   k, v = row
                   self.dict[k] = v
        except:
            pass
        with open(log_name, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
               k, v = row
               self.dict[k] = v
        return True

    def log_set(self, key, value):
        self.log_out.write("%s,%s\n" % (str(key), str(value)))