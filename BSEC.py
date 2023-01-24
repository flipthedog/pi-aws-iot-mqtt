# Interfaces with bme68x Python C extension

# BSEC Imports 
from bme68x import BME68X
import bme68xConstants as cst
import bsecConstants as bsec

# AWS Connector imports
from AWSMQTTConnector import AWSMQTTConnector

from datetime import datetime
import yaml
import time
import json

from threading import Timer

class BSEC:

    def __init__(self) -> None:
        
        # Load YAML config file
        with open("aws_details.conf", mode="rt", encoding="utf-8") as file:
            self.aws_config = yaml.safe_load(file)

        self.aws_con = AWSMQTTConnector(self.aws_config["endpoint"])

        self.bme = BME68X(cst.BME68X_I2C_ADDR_HIGH, 1)
        # self.bme.set_heatr_conf(cst.BME68X_FORCED_MODE, 320, 100, cst.BME68X_ENABLE)
        self.bme.set_sample_rate(bsec.BSEC_SAMPLE_RATE_LP)

        time.sleep(3)

        self.t = RepeatTimer(5, self.publish_loop)
        self.t.start()

    def get_data(self):
        
        data = None
        
        timeout = 3
        starttime = time.time()
        endtime = starttime

        while (data is None or data == {}) and (abs(endtime - starttime) < timeout):
            try:
                data = self.bme.get_bsec_data()
                # print(data)
            except Exception as e:
                print(e)
                return None

            time.sleep(0.1)

        return data
    
    def publish_to_aws(self, message):
        
        # publish_time
        # sample_nr
        # timestamp
        # iaq
        # iaq_accuracy
        # static_iaq
        # static_iaq_accuracy
        # co2_equivalent
        # breath_voc_equivalent
        # breath_voc_accuracy
        # raw_temperature
        # raw_pressure
        # raw_humidity
        # raw_gas
        # stabilization_status
        # run_in_status
        # temperature
        # humidity
        # gas_percentage
        # gas_percentage_accuracy

        message['publish_time'] = str(datetime.now())

        self.aws_con.publish_message(message, topic = "home/logger/atm_data")

    def publish_loop(self):

        timeout = 3

        starttime = time.time()
        endtime = starttime

        data = self.get_data()
        print(json.dumps(data))
        
        if data is not None and data != {}:
            self.publish_to_aws(data)
        else:
            print("didn't get data")
            raise Exception

    def debug_loop(self):
        
        while True:
            # {'sample_nr': 1, 'timestamp': 14984668289437, 'iaq': 50.0, 'iaq_accuracy': 0, 'static_iaq': 50.0, 'static_iaq_accuracy': 0, 
            # 'co2_equivalent': 600.0, 'co2_accuracy': 0, 'breath_voc_equivalent': 0.49999991059303284, 'breath_voc_accuracy': 0, 
            # 'raw_temperature': 13.764491081237793, 'raw_pressure': 100982.2421875, 'raw_humidity': 53.53776550292969, 
            # 'raw_gas': 9319.029296875, 'stabilization_status': 1, 'run_in_status': 1, 'temperature': 8.764491081237793, 
            # 'humidity': 74.54549407958984, 'gas_percentage': 0.0, 'gas_percentage_accuracy': 0}
            
            data = self.get_data()

            # data_dict = json.loads(data)
            if data is not None and data != {}:
                print(data)
                time.sleep(3)
            else:
                time.sleep(0.1)

    def __exit__(self):
        self.aws_con.__exit__()
        self.t.cancel()

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

if __name__ == '__main__':

    # debug
    bsec = BSEC()
    # bsec.debug_loop()