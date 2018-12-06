# prediction-loc

#### Setup
* Install google protobuf library:  
`pip3 install --user gtfs-realtime-bindings`  
`pip3 install --user protobuf`

* Install and setup AWS authentication with `awscli`:  
`pip3 install --user awscli`  
`aws configure`  
Enter "Access Key ID" and "Secret Access Key" provided by an AWS administrator

* Configure  
`export S3_BUCKET_NAME=bucket-name`

#### Usage

`python getArchive.py --datetime [YYYY-MM-DDTHH:mm] --output [output file location]`
