client = mqttClient.Client("Python3"+cloud9Lib.randomOnlyString(4))               
    # client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      
    client.on_message= on_message                      
    client.connect(broker_address, port=port)          
    client.loop_start()  
    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("exiting")
        client.disconnect()
        client.loop_stop()