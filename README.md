# prediction-loc

#### Setup
* Install google protobuf library:  
`pip3 install --user protobuf`

* Install and setup AWS authentication with `awscli`:  
`pip3 install --user awscli`  
`aws configure`  
Enter "Access Key ID" and "Secret Access Key" provided by an AWS administrator

#### Usage

`python getArchive.py --datetime [YYYY-MM-DDTHH:mm] --output [output file location]`
