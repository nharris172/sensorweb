"""Handles all interactions with the sensors and geospatial data
 in the database"""
import nclsensorweb.classes as cl
import nclsensorweb.maintenance as maintenance
import nclsensorweb.tools as tools
import nclsensorweb.errors as error
import psycopg2
from pygeocoder import Geocoder

class DatabaseConnection:
    """handles all database connections"""
    def __init__(self, host, db_name, user, password):
        connection_string = 'host=%s dbname=%s user=%s password = %s' \
         % (host, db_name, user, password)
        self.database = psycopg2.connect(connection_string)
        self.cur = self.database.cursor()

    def query(self, query_string):
        """queries the database and returns results"""
        self.cur.execute(query_string)
        results = self.cur.fetchall()
        return results
    
    def insert(self, insert_string):
        """inserts data into the database"""
        self.cur.execute(insert_string)
        self.database.commit()
        

class DataFunctions:
    def __init__(self, sensorweb):
        self.sensorweb = sensorweb
        
    def __check_reading(self,reading_name,units,reading_value):
        try:
            float(reading_value)
        except:
            return (False,None,)
        query_string = "select default_units, hstore_to_matrix(unit_conversion)\
         from readings where reading_name = '%s'" %(
        reading_name,)
        reading_info = self.sensorweb.database_connection.query(query_string)
        if not reading_info:
            
            return (False,None,)
        reading_info = reading_info[0]
        default_units = reading_info[0]
        units_conversion = {}
        if reading_info[1]:
            units_conversion = dict(reading_info[1])
        if units == default_units:
            return (True,float(reading_value))
        if units not in units_conversion.keys():
            return (False,None,)
        return (True,float(units_conversion[units])* float(reading_value),)
        
    def get_themes(self,):
        query_string  = "select distinct(theme) from readings"
        themes = self.sensorweb.database_connection.query(query_string)
        return [ item[0] for item in themes]
    
    def get_vars(self,tag=None,value=None):
        query_string  = "select reading_name from readings"
        if tag and value:
            query_string += " where %s = '%s'" % (tag,value)
        themes = self.sensorweb.database_connection.query(query_string)
        return [ item[0] for item in themes]
    
    def get_averages(self,variable_name,start_time,end_time,timedelta):
        averages =[]
        temp_time = start_time
        all_null = True
        while temp_time + timedelta<= end_time:
            stime,etime = temp_time,temp_time+timedelta
            query_string = "select hstore_to_matrix(info) from sensor_data where \
            info->'reading' = '%s' \
            and proper_timestamp(info->'timestamp') > '%s'\
            and proper_timestamp(info->'timestamp') <= '%s' \
            and not info?'flag' " % (variable_name,stime,etime)
            
            value_results = self.sensorweb.database_connection.query(query_string)
            average_values =[]
            for row in value_results:
                info = dict(row[0])
                reading_ok,value = self.__check_reading(info['reading'],
                                            info['units'],info['value'])
                
                if reading_ok:
                    average_values.append(float(value))
            average = None
            if average_values:
                all_null = False
                average= sum(average_values)/len(average_values)
            averages.append((tools.timestamp_to_timedelta(etime),average))
            
            temp_time +=timedelta
        if all_null:
            return None
        return averages

class SensorFunctions:
    """handles all sensor functions"""
    def __init__(self, sensorweb):
        self.sensorweb = sensorweb
        
    def __dict_to_sensor(self,info_dict):
            name = info_dict['name']
            id = info_dict['sensor_int_id']
            geom = info_dict['geom']
            active = info_dict['active']
            source = info_dict['source']
            type = info_dict['type']
            del info_dict['name'],info_dict['sensor_int_id'],info_dict['geom'],
            info_dict['active'],info_dict['source'], info_dict['type']
            return cl.Sensor(
                    self.sensorweb.database_connection,
                    name,
                    id,
                    geom, 
                    active, 
                    source, 
                    type,
                    info_dict
                    )

        
    def get_all(self, logged_in=False, active=True, not_flagged=True):
        """Retrieves relevant metadata for each 
        sensor stored in metadata database"""
        query_string = "select hstore_to_matrix(info) as info from sensors"
        clause = " info ? 'name'"
        if not logged_in:
            clause += " and info->'auth_needed' = 'False'"
        if active:
            clause += " and info->'active' = 'True' "
        if not_flagged:
            clause += " and info ? 'flag' = False "
        if clause:
            query_string += ' where %s' % (clause,)
        sens_row = self.sensorweb.database_connection.query(query_string)
        sensors = []
        for sens in sens_row:
            info = dict(sens[0])
            sensor = self.__dict_to_sensor(info)
            sensors.append(sensor)
        if sensors:
            return cl.SensorGroup(self.sensorweb.database_connection, sensors)

        
        
    def get(self, key, value,active=True, not_flagged=True):
        """retrives sensors matching the key value"""
        query_string = "select hstore_to_matrix(info) as info from sensors \
         where "
        clause = " info->'%s' = '%s'" % (key, value)
        if active:
            clause += " and info->'active' = 'True' "
        if not_flagged:
            clause += " and info ? 'flag' = False "
        if clause:
            query_string += '%s' % (clause,)
        sens_row = self.sensorweb.database_connection.query(query_string)
        sensors = []
        for sens in sens_row:
            info = dict(sens[0])
            sensor = self.__dict_to_sensor(info)
            sensors.append(sensor)
        if sensors:
            return cl.SensorGroup(self.sensorweb.database_connection, sensors)

    def create(self, _name, _geom, _type, _source, _active, _auth_needed,_extra={}):
        """creates a sensor entry"""
        new_type ,_type = self.sensorweb._check_tag('sensors','type',_type)
        new_source,_source = self.sensorweb._check_tag('sensors','source',_source)
        query_string = "select max(sensor_int_id_caster(info -> 'sensor_int_id'::text) ) from sensors"
        sens_id = self.sensorweb.database_connection.query(query_string)
        id = sens_id[0][0] +1
        sensor = cl.Sensor(
                    self.sensorweb.database_connection, 
                    _name,
                    id, 
                    _geom, 
                    _active, 
                    _source,
                    _type,
                    _extra)
                    
        sensor.link()
        if new_type:
            query_string = "update new_tags set info = case  when info is not null \
             then info||hstore('type','%s') else hstore('type','%s') end  \
             where table_name = 'sensors'" % (_type,_type)
            self.sensorweb.database_connection.insert(query_string)
        if new_source:
            query_string = "update new_tags set info = case  when info is not null \
             then info||hstore('source','%s') else hstore('source','%s') end  \
             where table_name = 'sensors'" % (_source,_source)
            self.sensorweb.database_connection.insert(query_string)
        return sensor
        
    def create_or_update(self, _name, _geom, _type,
                    _source, _active, _auth_needed,_extra ={}):
        """Creates sensor entry or updates one with with the matching name"""
        sensor =  self.get('name', _name,active=False, not_flagged=False)
        new_type ,_type = self.sensorweb._check_tag('sensors','type',_type)
        new_source,_source = self.sensorweb._check_tag('sensors','source',_source)
        if not sensor:
            sensor = self.create(_name, _geom, _type, 
                                _source, _active, _auth_needed)
        else:
            sensor = sensor[0]
        info_dict = {'geom':_geom, 'type':_type, 'source': _source,
                    'active':_active, 'auth_needed': _auth_needed}
        info_dict.update(_extra)
        sensor.update_info(info_dict)
        if new_type:
            query_string = "update new_tags set info = case  when info is not null \
             then info||hstore('type','%s') else hstore('type','%s') end  \
             where table_name = 'sensors'" % (_type,_type,)
            self.sensorweb.database_connection.insert(query_string)
        if new_source:
            query_string = "update new_tags set info = case  when info is not null \
             then info||hstore('source','%s') else hstore('source','%s') end  \
             where table_name = 'sensors'" % (_source,_source,)
            self.sensorweb.database_connection.insert(query_string)
        return sensor
    
    def get_tag_values(self,tag):
        """retrieves all the entries with the tag"""
        query_string = "select array_agg(distinct(info->'%s')) from sensors" %(tag,)
        response = self.sensorweb.database_connection.query(query_string)
        if response:
            return [str(item) for item in response[0][0]]
        
class GeospatialFunctions:
    """create Goesatial function class"""
    def __init__(self, sensorweb,):
        self.sensorweb = sensorweb
        
    def create_geospatial(self, geospatial_id, geom, theme, source, 
                            timestamp, reading, units, value, extra=None):
        """create geospatial class"""
        hstore = []
        hstore.append('"id"=>"%s"' %(geospatial_id,))
        hstore.append('"geom"=>"%s"' %(geom,))
        hstore.append('"source"=>"%s"' %(source,))
        hstore.append('"theme"=>"%s"' %(theme,))
        hstore.append('"timestamp"=>"%s"' %(timestamp,))
        hstore.append('"reading"=>"%s"' %(reading,))
        hstore.append('"units"=>"%s"' %(units,))
        hstore.append('"value"=>"%s"' %(value,))
        hstore.append('"special_tag"=>"GEO"')
        if extra:
            for key, val in extra.iteritems():
                hstore.append('"%s"=>"%s"' %(key, val))
        query_string = "insert into sensor_data (info) values ('%s')"\
                        % (','.join(hstore),)
        self.sensorweb.database_connection.insert(query_string)
        
    def get_all(self, starttime, endtime):
        """Retrives all geospatial entries between 2 times"""
        query_string = "select hstore_to_matrix(info) as info from sensor_data"
        clause = "info -> 'special_tag' = 'GEO' \
                and proper_timestamp(info->'timestamp') > '%s' \
                and proper_timestamp(info->'timestamp') < '%s'" % (starttime, endtime,)
                
        if clause:
            query_string += ' where %s' % (clause,)
        sens_row = self.sensorweb.database_connection.query(query_string)
        geo = []
        for sens in sens_row:
            info = dict(sens[0])
            geo.append(
                cl.Geospatial(
                    self.sensorweb.database_connection,
                    str(info['id']), 
                    str(info['geom']),
                    str(info['source']),
                    str(info['theme']),
                    str(info['timestamp']),
                    str(info['reading']),
                    str(info['units']),
                    str(info['value']),
                    info)
                    )
        return geo
    
    def get(self, starttime, endtime, key, value ):
        """Retrives all geospatial entries between 2 times"""
        query_string = "select hstore_to_matrix(info) as info from sensor_data"
        clause = "info -> 'special_tag' = 'GEO' \
                and proper_timestamp(info->'timestamp') > '%s' \
                and proper_timestamp(info->'timestamp') < '%s' and info->'%s'='%s'" % (starttime, endtime,key,value)
                
        if clause:
            query_string += ' where %s' % (clause,)
        sens_row = self.sensorweb.database_connection.query(query_string)
        geo = []
        for sens in sens_row:
            info = dict(sens[0])
            geo.append(
                cl.Geospatial(
                    self.sensorweb.database_connection,
                    str(info['id']), 
                    str(info['geom']),
                    str(info['source']),
                    str(info['theme']),
                    str(info['timestamp']),
                    str(info['reading']),
                    str(info['units']),
                    str(info['value']),
                    info)
                    )
        return geo
        
    
class SensorWeb:
    """SensorWeb class handles all interactions with the database"""
    def __init__(self, host, db_name, user, password, add_ons=None):
        self.database_connection = DatabaseConnection(host, db_name, 
                                                    user, password)
        self.sensors = SensorFunctions(self)
        self.geospatial = GeospatialFunctions(self)
        self.maintenance = maintenance.maintenance_class(self)
        self.data = DataFunctions(self)
        if add_ons:
            for add_on in add_ons:
                setattr(self, add_on.name, add_on(self))
                
    def _check_tag(self,table_name,tag_name,tag_value):
        query_string = "select info->'%s' from tags where table_name = '%s'" \
                        % (tag_name,table_name)
        existing_tags = self.database_connection.query(query_string)[0][0].strip('{}')
        existing_tags = existing_tags.replace('"','')
        existing_tags = existing_tags.split(',')
        tag_scores = {}
        for ex_tag in existing_tags:
            tag_scores[tools.levenshtein(ex_tag,tag_value)] = ex_tag
        min_score = min(tag_scores.keys())
        if min_score < 4:

            return False,tag_scores[min_score]
        
        return True,' '.join([part.capitalize() for part in tag_value.split(' ')])

    def make_geom(self, lat, lon):
        """creates postgis binary geometry from lat and lon"""
        query_string = "select ST_SetSRID(ST_MakePoint(%s,%s),4326)" \
                        % (lon, lat,)
        geom = self.database_connection.query(query_string)
        return geom[0][0]
    
    def geocode_placename(self,name):
        geom_results = Geocoder.geocode(name)
        lat,lon = geom_results[0].coordinates
        geom_binary = self.make_geom(lat,lon)
        return geom_binary
    
    def wkt_to_geom(self, wkt):
        """converts WKT to postgis binary geometry"""
        query_string = "select ST_GeomFromText('%s',4326)" % (wkt,)
        geom = self.database_connection.query(query_string)
        return geom[0][0]
