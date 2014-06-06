"""Class for sensor web"""
import simplejson
import nclsensorweb.tools as sensor_tools
import datetime

DATETIME_STRFORMAT = '%Y-%m-%d %H:%M:%S'
     
class SensorGroup:
    """Class for group of sensors"""
    def __init__(self,database_connection,_sensors):
        self.database = database_connection
        self.sensors = _sensors
        
    def get_latest(self,):
        lastest_times =[]
        i = 0
        for sensor in self.sensors:
            print i
            i+=1
            time = sensor.last_record()
            if time:
                lastest_times.append(time)
        if lastest_times:
            return max(lastest_times)
        
    def __iter__(self,):
        return (sensor for sensor in self.sensors)
    
    def __getitem__(self,index):
        return self.sensors[index]

class Sensor:
    """Sensor class"""
    def __init__(self, database_connection, _name,
                                _sensor_id,_geom, 
                                    _active, _source,
                                    _type, _extra):
        self.database = database_connection
        self.name = _name
        self.sensor_id = _sensor_id
        self.active = _active
        self.source = _source
        self.type = _type
        self._raw_geom = _geom
        self.extra_info = _extra
        query_string = "select ST_AsGeoJSON('%s')" % (_geom,)
        query_results = self.database.query(query_string)
        self.geom = simplejson.loads(query_results[0][0])
        self.latest =False
        
        
    def __check_reading(self,reading_name,units,reading_value):
        query_string = "select default_units, hstore_to_matrix(unit_conversion)\
         from readings where reading_name = '%s'" %(
        reading_name,)
        reading_info = self.database.query(query_string)[0]
        default_units = reading_info[0]
        units_conversion = dict(reading_info[1])
        if units == default_units:
            return (True,reading_value)
        if units not in units_conversion.keys():
            return (False,None,)
        return (True,float(nits_conversion[units])* reading_value,)
    
    def __default_units(self,reading_name):
        query_string = "select default_units from readings \
         where reading_name = '%s'" %(
        reading_name,)
        return self.database.query(query_string)[0][0]
        
    def link(self,):
        """links sensor to the database"""
        hstore = []
        hstore.append('"name"=>"%s"' %(self.name,))
        hstore.append('"sensor_int_id"=>"%s"' %(self.sensor_id,))
        hstore.append('"geom"=>"%s"' %(self._raw_geom,))
        hstore.append('"active"=>"%s"' %(self.active,))
        hstore.append('"source"=>"%s"' %(self.source,))
        hstore.append('"type"=>"%s"' %(self.type,))
        for key,value in self.extra_info:
            hstore.append('"%s"=>"%s"' %(key,value))
        insert_string = "insert into sensors ( info)  values ('%s')" \
                                                        % (','.join(hstore),)
        self.database.insert(insert_string)
        
    def update_info(self, infodict):
        """updates sensor information"""
        hstore = []
        
        for key, val in infodict.iteritems():
            
            hstore.append('"%s"=>"%s"' %(key, val,))
        
        insert_string = " update sensors set info = info||'%s'::hstore \
                            where info ->'name' = '%s' " \
                            % (','.join(hstore), self.name)
        self.database.insert(insert_string)
        
    def add_data(self, timestamp, reading, units, value, extra):
        """adds sensor data reading to database"""
        hstore = []
        hstore.append('"sensor_id"=>"%s"' %(self.sensor_id,))
        hstore.append('"timestamp"=>"%s"' %(timestamp,))
        hstore.append('"reading"=>"%s"' %(reading,))
        hstore.append('"units"=>"%s"' %(units,))
        hstore.append('"value"=>"%s"' %(value,))
        hstore.append('"raw"=>"True"' )
        for key, val in extra.iteritems():
            hstore.append('"%s"=>"%s"' %(key, val,))
        insert_string = "insert into sensor_data (info) values ('%s')"\
                                                        % (','.join(hstore),)
        self.database.insert(insert_string)
        
        
    def get_readings(self,):
        query_string = "select distinct(info->'reading') from sensor_data where sensor_int_id_caster(info -> 'sensor_id'::text) = %s" % (self.sensor_id,)
        return [str(item[0]) for item in self.database.query(query_string)]
    
    def last_record(self,):
        if self.latest != False:
            return self.latest

        query_string  =  " select proper_timestamp(info->'timestamp') from sensor_data \
                                    where sensor_int_id_caster(info -> 'sensor_id'::text) = %s order by \
                                    proper_timestamp(info->'timestamp') desc limit 2" % (self.sensor_id)
        response = self.database.query(query_string)
        if response:
            self.latest = response[0][0]
            return response[0][0]
        self.latest = None
        
    def get_data(self, starttime, endtime):
        """retrieves all the data entries for a sensor between 2 times"""
        data_list = []
        _var_info = {}
        query_string = "select hstore_to_matrix(info) from sensor_data where \
                       proper_timestamp(info->'timestamp') \
                        > '%s' and proper_timestamp(info->'timestamp') < '%s' \
                         and sensor_int_id_caster(info -> 'sensor_id'::text) = %s\
                         and not info?'flag' order by proper_timestamp(info->'timestamp')" \
                        % (starttime, endtime, self.sensor_id,)
        for row in self.database.query(query_string):
            
            info = dict(row[0])
            
            units = self.__default_units(info['reading'])
            
            variable = Variable(info['reading'], units, info['theme'])
            _var_info[info['reading']] = _var_info.get(info['reading'],
                                               {'variable':variable,'data':[]})
            if info['value']:
                reading_ok,value = self.__check_reading(info['reading'],
                                                        info['units'],info['value'])
                if reading_ok:
                    _var_info[info['reading']]['data'].append([
                        datetime.datetime.strptime(info['timestamp'],
                                                    DATETIME_STRFORMAT),
                        float(value)
                        ])
        
        for data in _var_info.values():
            if data['data']:
                data_list.append(Data(data['variable'], data['data']))
        return data_list
    
    def add_readings_from_dict(self,readings_dict):
        for name,info in readings_dict.iteritems():
            if 'extra' not in info.keys():
                info['extra'] ={}
            self.add_data(info['timestamp'],name,info['units'],info['value'],info['extra'])
    
    def json(self,):
        """return json of sensor object"""
        return {'source':self.source, 'type':self.type, 'active':self.active, 
                'geom':self.geom, 'name':self.name}
        

        
class Data:
    """Data entry for sensor"""
    def __init__(self, _var, _data):
        self.var = _var
        self.data = _data
        
    def latest_time(self,):
        """return latest reading time"""
        return self.data[-1][0]
        
    def live(self,):
        """return latest data value"""
        return self.data[-1][1]
        
    def json(self,):
        """creates json from data entry"""
        _json_data = []
        for row in self.data:
            _json_data.append([sensor_tools.timestamp_to_timedelta(row[0]),
                                                                    row[1]])
        return _json_data
        
class Variable:
    """variable"""
    def __init__(self, _name, _units, _theme):
        self.name = _name
        self.units = _units
        self.theme = _theme
        
class Geospatial:
    """Geostaptial data entry"""
    def __init__(self, database_connection, _name, _geom, _source, 
                _type, _timestamp, _reading, _units, _value, _extra):
        self.database = database_connection
        self.name = _name
        self.source = _source
        self.type = _type
        self._raw_geom = _geom
        query_string = "select ST_AsGeoJSON('%s')" % (_geom, )
        query_results = self.database.query(query_string)
        self.geom = simplejson.loads(query_results[0][0])
        self.timestamp = datetime.datetime.strptime(_timestamp.split('.')[0], 
                                                    DATETIME_STRFORMAT)
        self.reading = _reading
        self.units = _units
        self.value = _value
        self.extra = _extra
        
    def json(self,):
        """return json of geospatial element"""
        return {'source':self.source, 'type':self.type, 'geom':self.geom,
                'name':self.name, 'reading':self.reading, 'value':self.value,
                'units':self.units, 
                'time':{
                    'string':str(self.timestamp), 
                    'json':sensor_tools.timestamp_to_timedelta(self.timestamp)
                    }
                }
    
        