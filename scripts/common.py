import csv
import datetime as dt

def parse_csv_to_generator(filepath):

    with open(filepath) as file:
        reader = csv.reader(file)
        next(reader, None)  # skip the headers
        yield from reader


def parse_parameters(s3_bucket, s3_key):

    parsed_key = {}
    split_key = s3_key.split("/")
    if len(split_key) > 4 and "study_id" in s3_key:
        parsed_key["bucket"] = s3_bucket
        parsed_key["key"] = s3_key
        parsed_key["client"] = split_key[0]
        parsed_key["schema"] = parsed_key["client"]
        parsed_key["file"] = split_key[-1]
        parsed_key["file_name"] = parsed_key["file"].split(".")[0]
        parsed_key["table"] = split_key[-2]
        parsed_key["full_path"] = f"s3://{s3_bucket}/{s3_key}"
        file_format = str(parsed_key["full_path"]).split(".")
        parsed_key["modified_full_path"] = '.'.join([file_format[0],"modified",file_format[1]])
        parsed_key["modified_key"] = parsed_key["modified_full_path"].replace(f"s3://{s3_bucket}/","")
        parsed_key["created_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        parsed_ids = dict(path.split("=",1) for path in split_key if "=" in path)
        parsed_key = parsed_key | parsed_ids
    else:
        raise Exception("Invalid S3 key!")

    return parsed_key
