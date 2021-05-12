
try:
     api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        class config :
            tables = ['CYCLING_OUTDOOR', 'CYCLING_INDOOR']
            schema = 'schema'
            from_year = 2019
            to_year = 2020

        def send(port,msg):
            if isinstance(msg,str) :
                print('PORT {}: {}'.format(port,msg))
            else :
                print('PORT {}: \nattributes: {}\nbody: {}'.format(port,str(msg.attributes),str(msg.body)))

        def add_generator(func):
            func()

def log(log_str) :
    #api.logger.info(log_str)
    api.send('log',log_str)


def gen():
    
    tables = api.config.tables
    schema = api.config.schema
    
    # loop over tables and years
    for i,t in enumerate(tables) : 
        
        schematable = schema + '.' + t
        lastMessage = False
            
        #top = 'TOP 100000' 
        top = ''
        for year in range(api.config.from_year, api.config.to_year+1) :

            if t == 'CYCLING_OUTDOOR' :
                sql = 'SELECT {} TRAINING_ID, "date" as DATE,"timestamp" as TIMESTAMP, '\
                      '"distance" as DISTANCE, "heart_rate" as HEART_RATE, "cadence" as CADENCE, ' \
                      '"power" as POWER, "temperature" as TEMPERATURE '\
                      'FROM {} WHERE YEAR("timestamp") = {};'.format(top,schematable,year)
            elif t == 'CYCLING_INDOOR' :
                sql = 'SELECT {} TRAINING_ID, "date" as DATE,"timestamp" as TIMESTAMP, '\
                      '"heart_rate" as HEART_ATE, "cadence" as CADENCE, ' \
                      '"power" as POWER, "temperature" as TEMPERATURE '\
                      'FROM {} WHERE YEAR("timestamp") = {};'.format(top,schematable,year)
            elif t == 'RUNNING' :
                sql = 'SELECT {} TRAINING_ID, "date" as DATE,"timestamp" as TIMESTAMP, '\
                      '"distance" as DISTANCE, "heart_rate" as HEART_RATE, "cadence" as CADENCE, ' \
                      '"speed" as POWER, "temperature" as TEMPERATURE '\
                      'FROM {} WHERE YEAR("timestamp") = {};'.format(top,schematable,year)
            elif t in ['SWIMMING_POOL', 'SWIMMING_OPEN_WATER'] :
                sql = 'SELECT {} TRAINING_ID, "date" as DATE,"timestamp" as TIMESTAMP, '\
                      '"distance" as DISTANCE, "heart_rate" as HEART_RATE, "cadence" as CADENCE, ' \
                      '"speed" as POWER '\
                      'FROM {} WHERE YEAR("timestamp") = {};'.format(top,schematable,year)
            else :
                log('Sport type not supported: {}'.format(t))
                continue
            
            lastMessage = False 
            if i == len(tables) - 1 and year == api.config.to_year :
                lastMessage == True 
                                
            att = {'schema':schema,'table_name' : t,'lastMessage':lastMessage, \
                   'year':year,'table':{'name':schematable,'version':1}}
                   
            api.send("sql", api.Message(attributes = att  , body = sql))
            
            log('SQL send - {} : {}'.format(att['table_name'],sql))


api.add_generator(gen)



