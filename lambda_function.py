import json
import boto3
import time, datetime

def lambda_handler(event, context):
    # TODO implement
    
    print("event:", event)
    print("hello:", context)
    
    # {'sample_nr': 1, 'timestamp': 14984668289437, 'iaq': 50.0, 'iaq_accuracy': 0, 'static_iaq': 50.0, 'static_iaq_accuracy': 0, 
    # 'co2_equivalent': 600.0, 'co2_accuracy': 0, 'breath_voc_equivalent': 0.49999991059303284, 'breath_voc_accuracy': 0, 
    # 'raw_temperature': 13.764491081237793, 'raw_pressure': 100982.2421875, 'raw_humidity': 53.53776550292969, 
    # 'raw_gas': 9319.029296875, 'stabilization_status': 1, 'run_in_status': 1, 'temperature': 8.764491081237793, 
    # 'humidity': 74.54549407958984, 'gas_percentage': 0.0, 'gas_percentage_accuracy': 0}

    
    client = boto3.client('timestream-write')
    
    dimensions = [
        {
            'Name': 'data', 
            'Value': 'IOT',
        },
    ]
    
    d = datetime.datetime.now()
    
    atm_record = {
        'Dimensions': dimensions,
        'MeasureName': 'atm_data',
        'MeasureValueType': 'MULTI',
        'Time': str(int(time.mktime(d.timetuple())) * 1000 + d.microsecond),
        'TimeUnit': 'MILLISECONDS',
        'MeasureValues': [
                         {
                        'Name': 'sample_nr',
                        'Value': str(event['sample_nr']),
                        'Type': 'BIGINT',
                    },
                    {
                        'Name': 'iaq',
                        'Value': str(event['iaq']),
                        'Type': 'DOUBLE',
                    },                    {
                        'Name': 'iaq_accuracy',
                        'Value': str(event['iaq_accuracy']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'static_iaq',
                        'Value': str(event['static_iaq']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'static_iaq_accuracy',
                        'Value': str(event['static_iaq_accuracy']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'co2_equivalent',
                        'Value': str(event['co2_equivalent']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'co2_accuracy',
                        'Value': str(event['co2_accuracy']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'breath_voc_equivalent',
                        'Value': str(event['breath_voc_equivalent']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'breath_voc_accuracy',
                        'Value': str(event['breath_voc_accuracy']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'raw_temperature',
                        'Value': str(event['raw_temperature']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'raw_pressure',
                        'Value': str(event['raw_pressure']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'raw_humidity',
                        'Value': str(event['raw_humidity']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'stabilization_status',
                        'Value': str(event['stabilization_status']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'run_in_status',
                        'Value': str(event['run_in_status']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'temperature',
                        'Value': str(event['temperature']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'humidity',
                        'Value': str(event['humidity']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'gas_percentage',
                        'Value': str(event['gas_percentage']),
                        'Type': 'DOUBLE',
                    },
                    {
                        'Name': 'gas_percentage_accuracy',
                        'Value': str(event['gas_percentage_accuracy']),
                        'Type': 'DOUBLE',
                    },
                ]
    }
    
    records = [atm_record]
    
    response = client.write_records(
        DatabaseName='GinkgoHomeData',
        TableName='atm_data',
        Records=records, 
        CommonAttributes={}
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Pushed data to Timestream')
    }
