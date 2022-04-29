import random
import json
import pymysql
from paho.mqtt import client as mqtt_client
# # todo python监听报警
# MQTT相关参数
broker = '123.57.192.177'
port = 1883
topicOfPub = "cmd/123/rue/ABC"
topicOfSub1 = "data/123/rue/ABC"    ##定时数据主题
topicOfSub2 = "data/123/call/ABC"   ##报警数据主题
client_id = f'python-mqtt-{random.randint(0, 1000)}'

# todo python多线程，监听两个主题
# 数据库保存函数sqlsave(jsonData)，连接数据库whjcsp，并向XHDTESTing表中插入传来的json数据
def sqlsave(jsonData):
    db = pymysql.connect(
        host='123.57.192.177',      ##mysql数据库地址可以输入本机IP,可以是localhost
        port=3310,                  ##mysql数据库端口号
        user='whjcsp',              ##mysql数据库账号
        passwd='22173@CspSQL',      ##mysql数据库密码
        database='whjcsp')          ##mysql数据库库名

    cur = db.cursor()               ##使用 cursor() 方法创建一个游标对象 cur
    #sql语句对JSON格式数据处理
    sql = "INSERT INTO Lanting(insertTime,lng,lat,battery,id,type,switchType,qingjiao) \
               VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');" \
          % (jsonData['_terminalTime'], jsonData['lng'], jsonData['lat'], jsonData['battery'], jsonData['device_sn'],\
             jsonData['dev_type'],jsonData['kop'], jsonData['roll'])

    cur.execute(sql)                ##执行sql语句
    db.commit()                     ##插入数据
    print("数据已上传！")
    db.close()                      ##关闭数据库连接

# 接受到数据后处理
def on_message_come(client, userdata, msg):
    get_data = msg.payload          ##bytes  b'[s]，通过get_data获取mqtt中接受订阅消息的payload内容
    string = get_data.decode()
    msgjson = json.loads(string)    ##json.load()用于读文件，json.loads()用于读字符串；读取payload内容
    print(msgjson)
    sqlsave(msgjson)                ##调用数据库保存函数sqlsave(jsonData)

# 与MQTT服务器连接
def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# 订阅消息监听（1为状态/2为报警）
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(topicOfSub1)   ## 订阅的心跳主题参数
    client.on_message = on_message_come

# run
def run():
    # 连接MQTT
    client = connect_mqtt()
    # 监听消息
    subscribe(client)
    client.loop_forever()
    # 发布命令
    # client.loop_start()
    # publish_OnAndOff(client)

if __name__ == '__main__':
    run()


# import random
# import json
# import pymysql
# import time
# import threading
#
# from paho.mqtt import client as mqtt_client
#
# # MQTT相关参数
# broker = '123.57.192.177'
# port = 1883
# topicOfPub = "cmd/123/rue/ABC"
# topicOfSub1 = "data/123/rue/ABC"  ##定时数据监控主题
# topicOfSub2 = "data/123/call/ABC"  ##报警数据监控主题
# client_id = f'python-mqtt-{random.randint(0, 1000)}'
#
# # todo python多线程，监听两个主题
#
# # 数据库保存函数sqlsave(jsonData)，连接数据库whjcsp，并向XHDTESTing表中插入传来的json数据
# def sqlsave(jsonData):
#     db = pymysql.connect(
#         host='123.57.192.177',  ##mysql数据库地址可以输入本机IP,可以是localhost
#         port=3310,  ##mysql数据库端口号
#         user='whjcsp',  ##mysql数据库账号
#         passwd='22173@CspSQL',  ##mysql数据库密码
#         database='whjcsp')  ##mysql数据库库名
#
#     cur = db.cursor()  ##使用 cursor() 方法创建一个游标对象 cur
#     ##sql语句对JSON格式数据处理
#     ####2022/3/16 暂时检测方法
#     sql = "INSERT INTO Lanting(insertTime,lng,lat,battery,id,type,high,switchType,warnType) \
#                VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s');" \
#           % (jsonData['_terminalTime'], jsonData['lng'], jsonData['lat'], jsonData['battery'], jsonData['device_sn'],
#              jsonData['dev_type'], jsonData['high'], jsonData['kop'], jsonData['User_get_Alarmtype'])
#
#     cur.execute(sql)  ##执行sql语句
#     db.commit()  ##插入数据
#     print("数据已上传！")
#     db.close()  ##关闭数据库连接
#
#
# # 接受到数据后处理
# def on_message_come(client, userdata, msg):
#     get_data = msg.payload  ##bytes  b'[s]，通过get_data获取mqtt中接受订阅消息的payload内容
#     string = get_data.decode()
#     msgjson = json.loads(string)  ##json.load()用于读文件，json.loads()用于读字符串；读取payload内容
#     print(msgjson)
#     sqlsave(msgjson)  ##调用数据库保存函数sqlsave(jsonData)
#
#
# # 与MQTT服务器连接
# def connect_mqtt() -> mqtt_client:
#     def on_connect(client, userdata, flags, rc):
#         if rc == 0:
#             print("Connected to MQTT Broker!")
#         else:
#             print("Failed to connect, return code %d\n", rc)
#
#     client = mqtt_client.Client(client_id)
#     client.on_connect = on_connect
#     client.connect(broker, port)
#     return client
#
# # 发布消息命令
# def publish_OnAndOff(client):
#     msg_count = 0
#     while True:
#         time.sleep(1)
#         msg = f"messages: {msg_count}"
#         if msg_count == 0:
#             msg = f'{{"type":"set_var","payload":{{"kop":"1"}}'  ##开灯指令‘1’
#             msg_count = msg_count + 1
#         else:
#             msg = f'{{"type":"set_var","payload":{{"kop":"0"}}'  ##关灯指令‘0’
#             msg_count = msg_count - 1
#         result = client.publish(topicOfPub, msg)
#         # result: [0, 1]
#         status = result[0]
#         if status == 0:
#             print(f"Send `{msg}` to topic `{topicOfPub}`")
#         else:
#             print(f"Failed to send message to topic {topicOfPub}")
#
#
# # 订阅消息监听（1为状态/2为报警）
# def subscribe1(client: mqtt_client):
#     def on_message(client, userdata, msg):
#         print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#
#     client.subscribe(topicOfSub1)  ## 订阅的心跳主题参数
#     client.on_message = on_message_come
#
# def subscribe2(client: mqtt_client):
#     def on_message(client, userdata, msg):
#         print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#
#     client.subscribe(topicOfSub2)  ## 订阅的报警主题参数
#     client.on_message = on_message_come
#
# # run
# def run():
# # 连接MQTT
#    client1 = connect_mqtt()
# #     client2 = connect_mqtt()
#
# # 监听消息
#    subscribe1(client1)
#    client1.loop_forever()
# #     subscribe2(client2)
# #     client2.loop_forever()
# # 发布命令
#     # client.loop_start()
#     # publish_OnAndOff(client)
#
# if __name__ == '__main__':
#    t1 = threading.Thread(target=run)     # target是要执行的函数名（不是函数），args是函数对应的参数，以元组的形式存在
#    t1.start()
# #     t2 = threading.Thread(target=run)
# #     t2.start()
