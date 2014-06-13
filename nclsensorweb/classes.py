"""Class for sensor web"""
import simplejson
import nclsensorweb.tools as sensor_tools
import nclsensorweb.db_tools as db_tools
import datetime

DATETIME_STRFORMAT = '%Y-%m-%d %H:%M:%S'

class SensorData:
    def __init__(self,sensor,data_tables):
        self.sensor = sensor
        self.data_tables = data_tables
        self.__table_lookup = {}
        for table in self.data_tables:
            self.__table_lookup[table.var.name] = table
    def table(self,var_name):
        if var_name not in self.__table_lookup.keys():
            return None
        return  self.__table_lookup[var_name]
        
       
class SensorDataGroup:
    def __init__(self,sensor_data_list,variable_data,vars):
        self.sensor_data = sensor_data_list
        self.variable_data = variable_data
        self._var_steps = {}
        self.variables = vars
        self.__var_lookup = {}
        for var in vars:
            self.__var_lookup[var.name] = var
        self.__var_info ={}
        for var_name, values in variable_data.iteritems():
            values.sort()
            min_val,max_val = values[0], values[-1]
            self.__var_info[var_name] = {'min':min_val,'max':max_val,'avg':sum(values)/float(len(values))}
            if max_val == min_val:
                rng = 0
            else:
                rng = (max_val - min_val)/10
            self._var_steps[var_name] = []
            for i in range(11):
                self._var_steps[var_name].append(float(min_val+ rng*i))
        
    def level(self,var_name,value):
        return min(range(len(self._var_steps[var_name])), key=lambda i: abs(self._var_steps[var_name][i]-value))
    
    def heatmap(self,var_name):
        heatmap_obj = {'max':self.__var_info[var_name]['max'],'data':[]}
        for sensordata in self.sensor_data:
            table = sensordata.table(var_name)
            if table:
                heatmap_obj['data'].append({'count':table.live(),'geom':sensordata.sensor.geom})
        return heatmap_obj
    def variable_steps(self,):
        return self._var_steps

    def variable_summary(self,):
        var_summary = {}
        for var,var_info in self.__var_info.iteritems():
            var_summary[var] = var_info
            count = 0
            for sensor_data in self.sensor_data:
                if sensor_data.table(var):
                    count+=1
            var_summary[var]['num'] = count
            var_summary[var]['units'] = self.__var_lookup[var].units
        return var_summary
    def latest(self,):
        all_latest = []
        for sensordata in self.sensor_data:
            for table in sensordata.data_tables:
                all_latest.append(table.latest_time())
        all_latest.sort()
        return all_latest[-1]
        
class AverageSensorDataFunctions:
    def __init__(self,sensordatafunctions):
        self.sensordatafunctions = sensordatafunctions
        
    def get(self,starttime,endtime,timedelta):
        sensors_id = [ ]
        sensor_id_lookup = {}
        for sensor in self.sensordatafunctions.sensorgroup.sensors:
            sensors_id.append(sensor.sensor_id)
            sensor_id_lookup[sensor.sensor_id] = sensor
        query_string = "select hstore_to_matrix(info) from sensor_data where \
                       proper_timestamp(info->'timestamp') \
                        > '%s' and proper_timestamp(info->'timestamp') < '%s' \
                         and sensor_int_id_caster(info -> 'sensor_id'::text) in (%s)\
                         and not info?'flag' order by proper_timestamp(info->'timestamp')" \
                        % (starttime, endtime, ','.join(sensors_id),)
        print query_string
class SensorGroupDataFunctions:
    def __init__(self,sensorgroup):
        self.sensorgroup = sensorgroup
        self.average = AverageSensorDataFunctions(self)
    def latest(self,):
        lastest_times =[]
        for sensor in self.sensorgroup.sensors:
            time = sensor.last_record()
            if time:
                lastest_times.append(time)
        if lastest_times:
            return max(lastest_times)
        
    def get(self, starttime, endtime):
        
        sensors_id = [ ]
        sensor_id_lookup = {}
        for sensor in self.sensorgroup.sensors:
            sensors_id.append(sensor.sensor_id)
            sensor_id_lookup[sensor.sensor_id] = sensor
        query_string = "select hstore_to_matrix(info) from sensor_data where \
                       proper_timestamp(info->'timestamp') \
                        > '%s' and proper_timestamp(info->'timestamp') < '%s' \
                         and sensor_int_id_caster(info -> 'sensor_id'::text) in (%s)\
                         and not info?'flag' order by proper_timestamp(info->'timestamp')" \
                        % (starttime, endtime, ','.join(sensors_id),)
        
        __sensor_data = {}
        __variable_data = {}
        variables = {}
        checker = db_tools.ReadingChecker(self.sensorgroup.sensorweb.database_connection)
        for row in self.sensorgroup.sensorweb.database_connection.query(query_string):
            info = dict(row[0])
            units = checker.default_units[info['reading']]
            variable = Variable(info['reading'], units, info['theme'])
            variables[info['reading']] = Variable(info['reading'], units, info['theme'])
            __variable_data[info['reading']] = __variable_data.get(info['reading'],[])
            
            __sensor_data[info['sensor_id']] = __sensor_data.get(info['sensor_id'],{})
            __sensor_data[info['sensor_id']][info['reading']] = __sensor_data[info['sensor_id']].get(info['reading'],{'variable':variable,'data':[]})

            if info['value']:
                reading_ok,value = checker.check(
                    info['reading'],
                    info['units'],
                    info['value']
                            )
                if reading_ok:
                    __sensor_data[info['sensor_id']][info['reading']]['data'].append([
                        datetime.datetime.strptime(info['timestamp'].split('.')[0],
                                                    DATETIME_STRFORMAT),
                        float(value)
                        ])
                    __variable_data[info['reading']].append(float(value))
        sensor_data =[]
        for sensor_id, sensor_readings in __sensor_data.iteritems():
            data_list = []
            for data in sensor_readings.values():
                if data['data']:
                    data_list.append(Data(data['variable'], data['data']))
            sensor_data.append(SensorData(
                                sensor_id_lookup[sensor_id],
                                data_list
                                ))

        return SensorDataGroup(sensor_data,__variable_data,variables.values())
    
class SensorGroup:
    """Class for group of sensors"""
    def __init__(self,sensorweb,_sensors):
        self.sensorweb = sensorweb
        self.sensors = _sensors
        self.data = SensorGroupDataFunctions(self)
        
    def __iter__(self,):
        return (sensor for sensor in self.sensors)
    
    def __getitem__(self,index):
        return self.sensors[index]
    
    def types(self,):
        sensor_types= []
        for sensor in self.sensors:
            if sensor.type not in sensor_types:
                sensor_types.append(sensor.type)
        return sensor_types
    def sources(self,):
        sensor_sources= []
        for sensor in self.sensors:
            if sensor.source not in sensor_sources:
                sensor_sources.append(sensor.source)
        return sensor_sources

class SensorDataFunctions:
    def __init__(self,sensorid,db_connection):
        self.__sensor_id = sensorid
        self.__db_conn = db_connection
        self.__latest = False
        
    def add(self, timestamp, reading, units, value, theme, extra):
        """adds sensor data reading to database"""
        hstore = []
        hstore.append('"sensor_id"=>"%s"' %(self.__sensor_id,))
        hstore.append('"timestamp"=>"%s"' %(timestamp,))
        hstore.append('"reading"=>"%s"' %(reading,))
        hstore.append('"units"=>"%s"' %(units,))
        hstore.append('"value"=>"%s"' %(value,))
        hstore.append('"theme"=>"%s"' %(theme,))
        hstore.append('"raw"=>"True"' )
        for key, val in extra.iteritems():
            hstore.append('"%s"=>"%s"' %(key, val,))
        insert_string = "insert into sensor_data (info) values ('%s')"\
                                                        % (','.join(hstore),)
        self.__db_conn.insert(insert_string)
        db_tools.check_new_tags(self.__db_conn,reading,units)
        
    def variables(self,
        start_time=datetime.datetime.now() - datetime.timedelta(hours=24),
        end_time=datetime.datetime.now()):
        query_string = "select distinct(info->'reading') from sensor_data \
        where sensor_int_id_caster(info -> 'sensor_id'::text) = %s and\
         proper_timestamp(info->'timestamp') > '%s' \
        and proper_timestamp(info->'timestamp') < '%s'" % \
        (self.__sensor_id,start_time,end_time)
        return [str(item[0]) for item in self.__db_conn.query(query_string)]
        
    def latest(self,):
        if self.__latest != False:
            return self.latest

        query_string  =  " select proper_timestamp(info->'timestamp') from sensor_data \
                                    where sensor_int_id_caster(info -> 'sensor_id'::text) = %s order by \
                                    proper_timestamp(info->'timestamp') desc limit 2" % (self.__sensor_id)
        response = self.__db_conn.query(query_string)
        if response:
            self.__latest = response[0][0]
            return response[0][0]
        self.__latest = None
        
    def get(self, starttime, endtime):
        """retrieves all the data entries for a sensor between 2 times"""
        data_list = []
        _var_info = {}
        query_string = "select hstore_to_matrix(info) from sensor_data where \
                       proper_timestamp(info->'timestamp') \
                        > '%s' and proper_timestamp(info->'timestamp') < '%s' \
                         and sensor_int_id_caster(info -> 'sensor_id'::text) = %s\
                         and not info?'flag' order by proper_timestamp(info->'timestamp')" \
                        % (starttime, endtime, self.__sensor_id,)
                        
        for row in self.__db_conn.query(query_string):
            
            info = dict(row[0])
            
            units = db_tools.default_units(self.__db_conn,info['reading'])
            
            variable = Variable(info['reading'], units, info['theme'])
            _var_info[info['reading']] = _var_info.get(info['reading'],
                                               {'variable':variable,'data':[]})
            if info['value']:
                reading_ok,value = db_tools.check_reading(self.__db_conn,info['reading'],
                                                        info['units'],info['value'])
                if reading_ok:
                    _var_info[info['reading']]['data'].append([
                        datetime.datetime.strptime(info['timestamp'].split('.')[0],
                                                    DATETIME_STRFORMAT),
                        float(value)
                        ])
        
        for data in _var_info.values():
            if data['data']:
                data_list.append(Data(data['variable'], data['data']))
        return data_list
    
    def add_from_dict(self,readings_dict):
        for name,info in readings_dict.iteritems():
            if 'extra' not in info.keys():
                info['extra'] ={}
            self.add(info['timestamp'],name,info['units'],info['value'],info['theme'],info['extra'])
    
class Sensor:
    """Sensor class"""
    def __init__(self, database_connection, _name,
                                _sensor_id,_geom, 
                                    _active, _source,
                                    _type, _extra):
        self.__database = database_connection
        self.sensor_id = _sensor_id
        
        
        self.name = _name
        self.active = _active
        self.source = _source
        self.type = _type
        self._raw_geom = _geom
        
        query_string = "select ST_AsGeoJSON('%s')" % (_geom,)
        query_results = self.__database.query(query_string)
        self.geom = simplejson.loads(query_results[0][0])
        self.extra_info = _extra
        
        self.data = SensorDataFunctions(self.sensor_id,self.__database)
        
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
        self.__database.insert(insert_string)
        
    def update(self, infodict):
        """updates sensor information"""
        hstore = []
        
        for key, val in infodict.iteritems():
            
            hstore.append('"%s"=>"%s"' %(key, val,))
        
        insert_string = " update sensors set info = info||'%s'::hstore \
                            where info ->'name' = '%s' " \
                            % (','.join(hstore), self.name)
        self.__database.insert(insert_string)
    
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
    
        