import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node spotify_tracks_data_2023
spotify_tracks_data_2023_node1709402770548 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-sri/staging/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="spotify_tracks_data_2023_node1709402770548")

# Script generated for node spotify_albums_data_2023
spotify_albums_data_2023_node1709402770854 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-sri/staging/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="spotify_albums_data_2023_node1709402770854")

# Script generated for node spotify_artist_data_2023
spotify_artist_data_2023_node1709402771800 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-sri/staging/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="spotify_artist_data_2023_node1709402771800")

# Script generated for node JoinAA
JoinAA_node1709403368975 = Join.apply(frame1=spotify_artist_data_2023_node1709402771800, frame2=spotify_albums_data_2023_node1709402770854, keys1=["id"], keys2=["artist_id"], transformation_ctx="JoinAA_node1709403368975")

# Script generated for node JoinT
JoinT_node1709403423235 = Join.apply(frame1=spotify_tracks_data_2023_node1709402770548, frame2=JoinAA_node1709403368975, keys1=["id"], keys2=["track_id"], transformation_ctx="JoinT_node1709403423235")

# Script generated for node Drop Fields
DropFields_node1709411531954 = DropFields.apply(frame=JoinT_node1709403423235, paths=["`.id`"], transformation_ctx="DropFields_node1709411531954")

# Script generated for node Destination
Destination_node1709403729037 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1709411531954, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-sri/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1709403729037")

job.commit()