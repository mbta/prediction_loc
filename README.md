# prediction-loc

#### Setup
* Ensure you have `pipenv` installed:  
`brew install pipenv`

* Install the necessary libraries:  
`pipenv install`

* Install and setup AWS authentication with `awscli`:  
`pip3 install --user awscli`  
`aws configure`  
Enter "Access Key ID" and "Secret Access Key" provided by an AWS administrator

* Ensure certificates are set up.  
If you're using macOS go to Macintosh HD > Applications > Python folder > double click on "Install Certificates.command" file.

* Configure  
`export S3_BUCKET_NAME=bucket-name`
Bucket name is shared on LastPass

#### Usage

`python3 getArchive.py --datetime [YYYY-MM-DDTHH:mm] --output [output file location]`

###### Optional arguments

|       Argument       |                            Description                             |
| -------------------- | ------------------------------------------------------------------ |
| `--stop [stop id]`   | Use to only include trip_updates affecting the given stop_id       |
| `--route [route id]` | Use to only include trip_updates affecting the given route         |
| `--feed [name]`      | Feed to retrieve. Accepted values: `bus` (default), `subway`       |
| `--raw`              | Download the file as a raw protobuf instead of human-readable text |

#### Troubleshooting

If the `aws` command cannot be found, you will need to add it to your `PATH`:
1. Find the location of the install by running the command `python3 -c 'import awscli; print(awscli)'`
  - It should print something similar to  `<module 'awscli' from '/Users/localuser/Library/Python/3.6/lib/python/site-packages/awscli/__init__.py'\>`
2. Cut off the end of the filepath, starting with `lib`, and add `/bin` to get a path that looks like `/Users/localuser/Library/Python/3.6/bin`
3. Add that to your `PATH` by opening (or creating) `~/.bash_profile` and adding `export PATH=$PATH:/Users/localuser/Library/Python/3.6/bin` (using the path you created in step 2)
4. Open a new terminal window or run `source ~/.bash_profile` to apply your changes
