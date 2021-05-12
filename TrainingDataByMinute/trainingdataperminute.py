def gen():
    
    tables = api.config.tables
    
    for i,t in enumerate(tables) : 
        
        schema = api.config.schema
        
        if "CYCLING_OUTDOOR" in t : 
            sql = 'SELECT "TRAINING_ID", "date" as DATE, minute("timestamp") as MINUTE, '\
                  'hour("timestamp") as HOUR, ' \
                  'AVG("heart_rate") as HEART_RATE, AVG("power") as POWER, AVG("speed") as SPEED, '\
                  'AVG("cadence") as CADENCE FROM {}.{} '\
                  'GROUP BY "TRAINING_ID","date",hour("timestamp"),minute("timestamp")  '\
                  'ORDER BY "TRAINING_ID","date",hour("timestamp"),minute("timestamp")'.format(schema,t)
        elif "CYCLING_INDOOR" in t : 
            sql = 'SELECT "TRAINING_ID","date" as DATE, minute("timestamp") as MINUTE, '\
                  'hour("timestamp") as HOUR, ' \
                  'AVG("heart_rate") as HEART_RATE, AVG("power") as POWER, '\
                  'AVG("cadence") as CADENCE FROM {}.{} '\
                  'GROUP BY "TRAINING_ID","date",hour("timestamp"),minute("timestamp")  '\
                  'ORDER BY "TRAINING_ID","date",hour("timestamp"),minute("timestamp")'.format(schema,t)
        else : 
            sql = 'SELECT "TRAINING_ID","date" AS DATE, minute("timestamp") as MINUTE, '\
                  'hour("timestamp") as HOUR, ' \
                  'AVG("heart_rate") as HEART_RATE, AVG("speed") as SPEED, '\
                  'AVG("cadence") as CADENCE FROM {}.{} '\
                  'GROUP BY "TRAINING_ID","date",hour("timestamp"),minute("timestamp")  '\
                  'ORDER BY "TRAINING_ID","date",hour("timestamp"),minute("timestamp")'.format(schema,t)
        
        
        lastMessage = True if i == len(tables) - 1 else False
        api.send("sql", api.Message(attributes =  {'schema':schema,'table_name' : t,'lastMessage':lastMessage}, body = sql))


api.add_generator(gen)

