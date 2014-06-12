import nclsensorweb.db_tools as db_tools

class ReadingChecker:
    def __init__(self,db_connection):
        self.__db_conn = db_connection
        query_string = "select reading_name,default_units, \
        hstore_to_matrix(unit_conversion) from readings"
        self.__units_converter = {}
        self.default_units = {}
        for row in self.__db_conn.query(query_string):
            self.default_units[row[0]] = row[1]
            if row[2]:
               self.__units_converter[row[0]] = dict(row[2])
        
    def check(self,reading_name,units,reading_value):
        try:
            float(reading_value)
        except:
            return (False,None,)
        if reading_name not in self.default_units.keys():
            return (False,None,)
        if units == self.default_units[reading_name]:
            return (True,float(reading_value))
        if units not in self.__units_converter[reading_name].keys():
            return (False,None,)
        return (True,float(self.__units_converter[reading_name][units])* float(reading_value),)
        


def check_tag(db_connection,table_name,tag_name,tag_value):
    query_string = "select info->'%s' from tags where table_name = '%s'" \
                    % (tag_name,table_name)
    existing_tags = db_connection.query(query_string)[0][0].strip('{}')
    existing_tags = existing_tags.replace('"','')
    existing_tags = existing_tags.split(',')
    tag_scores = {}
    for ex_tag in existing_tags:
        tag_scores[tools.levenshtein(ex_tag,tag_value)] = ex_tag
    min_score = min(tag_scores.keys())
    if min_score < 4:

        return False,tag_scores[min_score]
    
    return True,' '.join([part.capitalize() for part in tag_value.split(' ')])

def check_reading(db_connection,reading_name,units,reading_value):
    try:
        float(reading_value)
    except:
        return (False,None,)
    query_string = "select default_units, hstore_to_matrix(unit_conversion)\
     from readings where reading_name = '%s'" %(
    reading_name,)
    reading_info = db_connection.query(query_string)
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

def default_units(db_conn,reading_name):
    query_string = "select default_units from readings \
     where reading_name = '%s'" %(
    reading_name,)
    return db_conn.query(query_string)[0][0]


def check_new_tags(db_conn,reading_name,units):
    query_string = "select default_units,akeys(unit_conversion)\
     from readings where reading_name = '%s'" %(
    reading_name,)
    reading_info = db_conn.query(query_string)
    new_reading = reading_info == []
    new_units = True
    if not new_reading:
        existing_units = [str(reading_info[0][0])]
        if reading_info[0][1]:
            existing_units += [str(item) for item in list(reading_info[0][1])]
        new_units = units not in existing_units
    if new_units:
        query_string = "insert into new_reading \
         (reading_name,units, new_reading) values ('%s','%s',%s)" %\
        ( reading_name,units,new_reading)
        db_conn.insert(query_string)
        
def get_tag_values(db_conn,table,tag):
    """retrieves all the entries with the tag"""
    query_string = "select array_agg(distinct(info->'%s')) from %s" %(tag,table,)
    response = db_conn.query(query_string)
    if response:
        return [str(item) for item in response[0][0]]