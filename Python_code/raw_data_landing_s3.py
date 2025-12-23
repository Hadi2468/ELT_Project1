import boto3
import os

## Connect to AWS S3
s3_resource = boto3.resource('s3', 'us-east-1')

## Loading real data 
local_data_folder = 'full_data'
s3_data_folder = 'data'

## Upload files to S3
def s3_upload(file_path, fold, bucket):
    s3_bucket = s3_resource.Bucket(name=bucket)
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    file_name_only = os.path.basename(file_path)

    s3_bucket.upload_file(
        Filename=file_path,
        Key=f"{fold}/{file_name_only}"
    )
    return True


if __name__ == '__main__':

    bucket = 'project-1-raw-data-bucket'

    uploads = [
        {"file_path": f"./Data/{local_data_folder}/fact.csv",       "s3_folder": s3_data_folder},
        {"file_path": f"./Data/{local_data_folder}/department.csv", "s3_folder": s3_data_folder},
        {"file_path": f"./Data/{local_data_folder}/stores.csv",     "s3_folder": s3_data_folder}
    ]

    for job in uploads:
        status = s3_upload(job["file_path"], job["s3_folder"], bucket)
        if status:
            print(f"✅ Uploaded: {job['file_path']}   \t →    S3:{bucket}/{job['s3_folder']}")
        else:
            print(f"❌ Failed uploading: {job['file_path']}")
