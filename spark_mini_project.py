from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class AutoIncidentProcessor:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.conf = SparkConf().setAppName("Auto Incident Processor")
        self.sc = SparkContext(conf=self.conf)
        self.spark = SparkSession(self.sc)

    def extract_vin_key_value(self, line):
        fields = line.split(',')
        vin = fields[2]
        incident_type = fields[1]
        make = fields[3] if incident_type == 'I' else ''
        year = fields[5] if incident_type == 'I' else ''
        return (vin, (incident_type, make, year))

    def populate_make(self, records):
        make = ''
        year = ''
        accidents = []
        
        for record in records:
            if record[0] == 'I':
                make = record[1]
                year = record[2]
            elif record[0] == 'A':
                accidents.append((make, year))
        
        return accidents

    def extract_make_key_value(self, record):
        return (f"{record[0]}-{record[1]}", 1)

    def process(self):
        # Step 1: Filter out accident incidents with make and year
        raw_rdd = self.sc.textFile(self.input_file)
        vin_kv = raw_rdd.map(self.extract_vin_key_value)
        enhance_make = vin_kv.groupByKey().flatMap(lambda kv: self.populate_make(kv[1]))

        # Step 2: Count number of occurrences for accidents for the vehicle make and year
        make_kv = enhance_make.map(self.extract_make_key_value)
        result = make_kv.reduceByKey(lambda a, b: a + b)

        # Step 3: Save the result to HDFS as text
        result.saveAsTextFile(self.output_file)

        self.sc.stop()

if __name__ == "__main__":
    input_file = "data.csv"
    output_file = "output"
    
    processor = AutoIncidentProcessor(input_file, output_file)
    processor.process()