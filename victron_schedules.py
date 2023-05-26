class Schedule:
    id: int = None
    start_time: int = None
    day: int = None
    duration: int = None

    def set_id(self, id: int):
        self.id = id

    def get_id(self):
        return self.id

    def set_start_time(self, start_time: int):
        self.start_time = start_time

    def get_start_time(self):
        return self.start_time
    
    def get_python_start_time(self):
        return int(self.start_time / 3600)
    
    def set_day(self, day: int):
        self.day = day

    def get_day(self):
        return self.day
    
    def get_python_day(self):
        # Python weekdays Monday == 0 ... Sunday == 6
        # Victron weekdays Sunday == 0 ... Saturday == 6
        return self.day -1 if self.day != 0 else + 6

    def set_duration(self, duration: int):
        self.duration = duration

    def get_duration(self):
        return self.duration

    def __str__(self):
        return f"Id = {str(self.id)}, Start Time = {str(self.start_time)}, Day = {str(self.day)}, Duration = {str(self.duration)}"