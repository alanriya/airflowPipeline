from abc import abstractmethod, ABC

class WeatherBase(ABC):
    def __init__(self, url):
        self.url = url

    def check_status(self):
        pass 
    
    @abstractmethod
    def retrieve_data(self) -> dict:
        pass













