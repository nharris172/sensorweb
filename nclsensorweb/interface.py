"""Handles all interactions with the sensors and geospatial data
 in the database"""
import nclsensorweb.classes as cl
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
        
    def get_all(self, logged_in=False):
        """Retrieves relevant metadata for each 
        sensor stored in metadata database"""
        query_string = "select hstore_to_matrix(info) as info from sensors"
        clause = "info->'active' = 'True' and info ? 'special_tag' = False"
        if not logged_in:
            clause += " and info->'auth_needed' = 'False'"
        if clause:
            query_string += ' where %s' % (clause,)
        sens_row = self.sensorweb.database_connection.query(query_string)
        sensors = []
        for sens in sens_row:
            info = dict(sens[0])
            sensors.append(
                cl.Sensor(
                    self.sensorweb.database_connection,
                    str(info['name']),
                    str(info['geom']),
                    str(info['active']),
                    str(info['source']),
                    str(info['type'])
                    )
                )
        return cl.SensorGroup(self.sensorweb.database_connection, sensors)

        
        
    def get(self, key, value):
        """retrives sensors matching the key value"""
        query_string = "select hstore_to_matrix(info) as info from sensors"
        clause = " info->'%s' = '%s'" % (key, value)
        if clause:
            query_string += ' where %s' % (clause,)
        sens_row = self.sensorweb.database_connection.query(query_string)
        sensors = []
        for sens in sens_row:
            info = dict(sens[0])
            sensors.append(
                cl.Sensor(
                    self.sensorweb.database_connection,
                    info['name'],
                    info['geom'], 
                    info['active'], 
                    info['source'], 
                    info['type']
                    )
                )
        return cl.SensorGroup(self.sensorweb.database_connection, sensors)

    def create(self, _name, _geom, _type, _source, _active, _auth_needed):
        """creates a sensor entry"""
        sensor = cl.Sensor(
                    self.sensorweb.database_connection, 
                    _name, 
                    _geom, 
                    _active, 
                    _source,
                    _type)
                    
        sensor.link()
        return sensor
        
    def create_or_update(self, _name, _geom, _type,
                    _source, _active, _auth_needed):
        """Creates sensor entry or updates one with with the matching name"""
        sensor =  self.get('name', _name)
        if not sensor:
            sensor = self.create(_name, _geom, _type, 
                                _source, _active, _auth_needed)
        else:
            sensor = sensor[0]
        info_dict = {'geom':_geom, 'type':_type, 'source': _source,
                    'active':_active, 'auth_needed': _auth_needed}
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

    def make_geom(self, lat, lon):
        """creates postgis binary geometry from lat and lon"""
        query_string = "select ST_SetSRID(ST_MakePoint(%s,%s),4326)" \
                        % (lon, lat,)
        geom = self.database_connection.query(query_string)
        return geom[0][0]
    
    def geocode_placename(self,name):
        geom_results = Geocoder.geocode(name)
        lat,lon = geom_results[0].coordinates
        print  lat,lon
        geom_binary = self.make_geom(lat,lon)
        return geom_binary
    
    def wkt_to_geom(self, wkt):
        """converts WKT to postgis binary geometry"""
        query_string = "select ST_GeomFromText('%s',4326)" % (wkt,)
        geom = self.database_connection.query(query_string)
        return geom[0][0]
