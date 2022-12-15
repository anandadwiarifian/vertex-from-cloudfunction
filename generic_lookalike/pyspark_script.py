import argparse
import sys
from pyspark.sql import SparkSession

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_table',
                        dest='source_table',
                        default=' ',
                        help='source table')
    parser.add_argument('--target_table',
                        dest='target_table',
                        default='dataset_test.generic_lookalike_test',
                        help='Target table')
    parser.add_argument('--feature_list',
                    dest='feature_list',
                    default='total_arpu,total_topups',
                    help='Feature to be used for lookalike')
    parser.add_argument('--msisdn_list',
                    dest='msisdn_list',
                    default='199,7')   
    parser.add_argument('--bucket',
                    dest='bucket',
                    default='vertex-testing-poc-123/generic_lookalike/temp',
                    help='temporary bucket')                                        
    
    known_args, _ = parser.parse_known_args(argv)
    
    spark = SparkSession\
        .builder\
        .appName("wordcount")\
        .getOrCreate()
    spark.conf.set('temporaryGcsBucket', known_args.bucket)
    
    # sc = spark.sparkContext    
    df_spark = spark.read \
        .format("bigquery") \
        .option("table", known_args.source_table) \
        .option("filter", f"msisdn IN ({known_args.msisdn_list})") \
        .load()
    
    feature_list = [i.strip() for i in (known_args.feature_list).split(",")]
    
    df_spark = df_spark.select(feature_list)

    df_spark.write.format('bigquery') \
        .option('table', known_args.target_table) \
        .option('mode', 'truncate') \
        .mode("append") \
        .save()
    
    spark.stop()
    
if __name__ == '__main__':
    run(sys.argv)