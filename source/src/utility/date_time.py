import datetime as dt

class DateTime():

    @staticmethod
    def generate_datetime_ranges(
        start_date:str=None, 
        end_date:str=None, 
        )->list:
        """ 
        Generates a range of datetime ranges. 
        - start_date: provide a str with the format "yyyy-mm-dd"
        - end_date: provide a str with the format "yyyy-mm-dd" 
        Usage example: 
        - generate_datetime_ranges(start_date="2020-01-01", end_date="2022-01-02")
            returns: 
                [
                    'start_time': '2020-01-01T00:00:00.00Z', 'end_time': '2020-01-02T00:00:00.00Z'}, 
                    {'start_time': '2020-01-02T00:00:00.00Z', 'end_time': '2020-01-03T00:00:00.00Z'}
                ]
        """

        date_range = []
        if start_date is not None and end_date is not None: 
            dte_start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
            dte_end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
            date_range = [
                {
                    "start_time": (dte_start_date + dt.timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%S.00Z"),
                    "end_time": (dte_start_date + dt.timedelta(days=i) + dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.00Z"),
                }
            for i in range((dte_end_date - dte_start_date).days)]
        else: 
            raise Exception("The parameters passed in results in no action being performed.")

        return date_range  