
class maintenance_class:
    def __init__(self,sensorweb):
        self.sensorweb = sensorweb
        
    def flag_suspect_values(self,):
        query_string = "select reading_name, hstore_to_matrix(flag_checker) from readings "
        reading_flag_checker = {}
        reading_flag_checker_rows = self.sensorweb.database_connection.query(query_string)
        for row in reading_flag_checker_rows:
            if row[1]:
                reading_flag_checker[row[0]]= dict(row[1])
        query_string = "select sensor__data_id,hstore_to_matrix(info) from sensor_data where info -> 'raw' = 'True'"
        raw_data_readings = self.sensorweb.database_connection.query(query_string)
        i = 0
        for row in raw_data_readings:
            i+=1
            row_number = row[0]
            info = dict(row[1])
            if info['reading'] not in reading_flag_checker.keys():
                query_string = "update sensor_data set info = delete(info,'raw') where sensor__data_id = %s " % ( row_number,)
                self.sensorweb.database_connection.insert(query_string)
                continue
            flag_value = False
            if 'min_val' in reading_flag_checker[info['reading']].keys():
                if float(info['value']) < float(reading_flag_checker[info['reading']]['min_val']):
                    flag_value = True
            if 'max_val' in reading_flag_checker[info['reading']].keys():
                if float(info['value']) > float(reading_flag_checker[info['reading']]['max_val']):
                    flag_value = True
            info_string = "delete(info,'raw')"
            if flag_value:
                info_string+="||hstore('flag','True')"
            query_string = "update sensor_data set info = %s where sensor__data_id = %s " % ( info_string,row_number,)
            self.sensorweb.database_connection.insert(query_string)
        