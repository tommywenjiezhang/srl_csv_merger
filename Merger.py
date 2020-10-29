from abc import ABCMeta, abstractmethod
from FileUtils import readCsvDir

class Merger(metaclass=ABCMeta):
    
    @abstractmethod
    def __init__(self,path):
        self.path = path
        self.all_files = readCsvDir(path)
    
    @abstractmethod
    def merge(self):
        pass
    
    @abstractmethod
    def show(self):
        pass
        

if __name__ == "__main__":
    Merger()