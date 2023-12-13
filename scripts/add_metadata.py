#!/usr/bin/env python

import pandas as pd
import sys
import json

"""
For each line - Read each json object, perform sentiment analysis on
the tweet text, add sentiment field to json object, append to parent json object 
and write the parent json to a file.
"""

def add_metadata(input, output, args):
    df = pd.read_csv(input)
    df["study_id"] = args["study_id"]
    df["budget_id"] = args["budget_id"]
    if args.get("supportive_document_id") != None:
        df["supportive_document_id"] = args["supportive_document_id"]
    df["created_at"] = args["created_at"]

    df.to_csv(output)

input,output,args = sys.argv[1],sys.argv[2],json.loads(sys.argv[3].replace("\'", "\""))

add_metadata(input, output, args)
