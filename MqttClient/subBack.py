import random
import json
import pymysql
from paho.mqtt import client as mqtt_client
# MQTT相关参数
broker = '123.57.192.177'
port = 1883
topicOfPub = "cmd/123/rue/ABC"
topicOfSub1 = "data/123/rue/ABC"    ##定时数据主题
topicOfSub2 = "data/123/call/ABC"   ##报警数据主题
topicOfSub3 = "data/123/back/ABC"   ##反馈数据主题
client_id = f'python-mqtt-{random.randint(0, 1000)}'

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
    sql = "INSERT INTO Lanting(insertTime,lng,lat,battery,id,type,switchType,warnType,qingjiao,alarm) \
               VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" \
          % (jsonData['_terminalTime'], jsonData['lng'], jsonData['lat'], jsonData['battery'], jsonData['device_sn'],\
             jsonData['dev_type'],jsonData['kop'],jsonData['Alarmtype'],jsonData['roll'],jsonData['Alarm'])

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

    client.subscribe(topicOfSub3)   ## 订阅的报警主题参数
    client.on_message = on_message_come

def run():
    # 连接MQTT
    client = connect_mqtt()
    # 监听消息
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()
