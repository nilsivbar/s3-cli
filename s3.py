#!/usr/bin/env python3

import boto3
import boto.s3.connection
import configparser
import os

import argparse

from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("-ls", help="list buckets", action="store_true")
parser.add_argument("-mb", help="create bucket", action="store_true")
parser.add_argument("-rb", help="delete bucket", action="store_true")
parser.add_argument("-vb", help="enable versioning of bucket", action="store_true")
parser.add_argument("-dvb", help="disable versioning of bucket", action="store_true")
parser.add_argument("-lsb", help="list versioned objects in bucket", action="store_true")
parser.add_argument("-ub", help="upload file to bucket", action="store_true")
parser.add_argument("-do", help="delete object", action="store_true")
parser.add_argument("-vi", help="version id(specify all to del all version)")
parser.add_argument("-fname", help="filename name")
parser.add_argument("-name", help="bucket name")

args = parser.parse_args()

config = configparser.ConfigParser()
config.read(str(Path.home())+'/.s3cfg')

access_key = config['default']['access_key']
secret_key = config['default']['secret_key']

if config['default']['use_https'] == "True":
    is_secure = True
else:
    is_secure = False

session = boto3.session.Session()

conn = session.client(
    service_name='s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url='https://'+config['default']['host_base'],
)

if args.ls:
    res = conn.list_buckets()
    print("Buckets:")
    for bucket in res['Buckets']:
        res = conn.get_bucket_versioning(Bucket=bucket['Name'])
        print(bucket['Name'] + "	Versioning: " + res['Status'])
    exit(0)

if args.mb:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    response = conn.create_bucket(
        ACL='private',
        Bucket=args.name
    )

if args.rb:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    conn.delete_bucket(
        Bucket=args.name
    )

if args.vb:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    response = conn.put_bucket_versioning(
        Bucket=args.name,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )

if args.dvb:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    response = conn.put_bucket_versioning(
        Bucket=args.name,
        VersioningConfiguration={
            'Status': 'Suspended'
        }
    )

if args.lsb:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    response = conn.list_object_versions(
        Bucket=args.name
    )
    for object in response['Versions']:
        print(object['Key'] + " modified: " +
              str(object['LastModified']) + " VersionId: " +
              object['VersionId'] + " Latest: " + str(object['IsLatest']))

if args.ub:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    if len(args.fname) <= 0:
        print("No filename given")
        exit(1)
    head, tail = os.path.split(args.fname)
    with open(args.fname, 'rb') as data:
        conn.upload_fileobj(data, args.name, tail)

if args.do:
    if len(args.name) <= 0:
        print("No bucket name given")
        exit(1)
    if len(args.fname) <= 0:
        print("No filename given")
        exit(1)
    if len(args.vi) <= 0:
        print("No VersionId given")
        exit(1)

    response = conn.delete_object(
        Bucket=args.name,
        Key=args.fname,
        VersionId=args.vi
    )
