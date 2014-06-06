"""Handles all interactions with the sensors and geospatial data
 in the database"""
import nclsensorweb.classes as cl
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
        clause = "info ? 'special_tag' "
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
        new_type ,_type = self.sensorweb.check_tag('sensors','type',_type)
        new_source,_source = check_tag('sensors','source',_source)
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
        return sensor
        
    def create_or_update(self, _name, _geom, _type,
                    _source, _active, _auth_needed,_extra ={}):
        """Creates sensor entry or updates one with with the matching name"""
        sensor =  self.get('name', _name,active=False, not_flagged=False)
        if not sensor:
            sensor = self.create(_name, _geom, _type, 
                                _source, _active, _auth_needed)
        else:
            sensor = sensor[0]
        info_dict = {'geom':_geom, 'type':_type, 'source': _source,
                    'active':_active, 'auth_needed': _auth_needed}
        info_dict.update(_extra)
        sensor.update_info(info_dict)
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
        
    
class SensorWeb:
    """SensorWeb class handles all interactions with the database"""
    def __init__(self, host, db_name, user, password, add_ons=None):
        self.database_connection = DatabaseConnection(host, db_name, 
                                                    user, password)
        self.sensors = SensorFunctions(self)
        self.geospatial = GeospatialFunctions(self)
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
