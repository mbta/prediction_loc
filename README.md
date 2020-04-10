# prediction-loc

#### Setup
* Ensure you have [Python 3](https://www.python.org/downloads/) installed.
* Install helper software:
  * Mac Users:
    1. Install Homebrew according to instructions at https://brew.sh
    2. Install Pipenv with `brew install pipenv`
  * Windows users:
    1. Install Pipenv with `pip3 install --user pipenv`


* Install the necessary libraries. Run this command in the predictionloc directory:
`pipenv install`

* Install and setup AWS authentication with `awscli`:
  - `pip3 install --user awscli`
  - `aws configure`

  When prompted, enter "Access Key ID" and "Secret Access Key" provided by an AWS administrator. See "Troubleshooting" section below if you can't get `aws configure` to run.
  - enter 'us-east-1' for region name
  - hit the enter/return key when prompted to enter a default output format  
  

* Ensure certificates are set up.
If you're using macOS go to Macintosh HD > Applications > Python folder > double click on "Install Certificates.command" file.

* Configure
`export S3_BUCKET_NAME=bucket-name`
Bucket name is shared on LastPass

#### Usage

`pipenv run getArchive --datetime [YYYY-MM-DDTHH:mm]`

###### Optional arguments

|        Argument       |                                          Description                                          |
| --------------------- | --------------------------------------------------------------------------------------------- |
| `--stop [stop id]`    | Use to only include trip_updates affecting the given (comma-separated) stop_id(s)             |
| `--route [route id]`  | Use to only include trip_updates affecting the given route                                    |
| `--trip [trip id]`    | Use to only include a specific trip_id                                                        |
| `--feed [name]`       | Feed to retrieve. Accepted values: `bus` (default), `subway`, `cr`, `winthrop`, `concentrate` |
| `--raw`               | Download the file directly, without filtering or processing                                   |
| `--output [filepath]` | Where to create the output file (default is `prediction-loc/output/[feed]-[datetime].json`)   |
| `--f busloc_vehicle`  | Use to see all vehicles in our feed at the time                                                  |

* Note: route_id is matched exactly for the `bus` and `concentrate` feeds, but does fuzzy matching for all others.
For example, `--route Green` will include all Green Line branches, and `--route Worcester` will still match route_id
`CR-Worcester`

#### Troubleshooting

If the `aws` command cannot be found, you will need to add it to your `PATH`:
1. Find the location of the install by running the command `python3 -c 'import awscli; print(awscli)'`
  - It should print something similar to  `<module 'awscli' from '/Users/localuser/Library/Python/3.6/lib/python/site-packages/awscli/__init__.py'\>`
2. Cut off the end of the filepath, starting with `lib`, and add `/bin` to get a path that looks like `/Users/localuser/Library/Python/3.6/bin`
3. Add that to your `PATH` by opening (or creating) `~/.bash_profile` and adding `export PATH=$PATH:/Users/localuser/Library/Python/3.6/bin` (using the path you created in step 2)
4. Open a new terminal window or run `source ~/.bash_profile` to apply your changes
