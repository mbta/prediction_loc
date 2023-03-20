# prediction-loc

Downloads archived MBTA real-time data feeds from various sources.

## Prerequisites

* Python 3
  * **Mac/Linux:** can install using [asdf](https://asdf-vm.com/) (`asdf install` in this directory)
  * **Mac:** can install from [Homebrew](https://brew.sh) (`brew install python3 pipenv`)
  * **Windows:**
    * download from [official site](https://www.python.org/downloads/windows/)
    * install Pipenv: `pip3 install --user pipenv`
* AWS CLI
  * **Mac:** can install from Homebrew (`brew install awscli`)
  * **Any:** can install as a global Python package (`pip3 install --user awscli`)
  * Use `aws configure` to set up your credentials:
    * "Access Key ID" and "Secret Access Key" are provided by an AWS admin
    * Enter "Region" as `us-east-1`
    * Press Enter when prompted for a default output format

## Setup

* `pipenv install` in this directory
* `export S3_BUCKET_NAME=x`, replacing `x` with bucket name stored in 1Password

## Clone and step into the Github directory
* `git clone https://github.com/mbta/prediction_loc.git`
* `cd prediction_loc`

## Usage

`pipenv run getArchive --datetime [YYYY-MM-DDTHH:mm]`

### Optional arguments

|        Argument           |                                          Description                                          |
| ------------------------- | --------------------------------------------------------------------------------------------- |
| `--stop [stop id]`        | Use to only include trip_updates affecting the given (comma-separated) stop_id(s)             |
| `--route [route id]`      | Use to only include trip_updates affecting the given route                                    |
| `--trip [trip id]`        | Use to only include a specific trip_id                                                        |
| `--feed [name]`           | Feed to retrieve. Accepted values: `bus` (default), `subway`, `cr`, `cr_vehicle`, `cr_boarding`, `winthrop`, `concentrate`, `concentrate_vehicle`, `alerts`, `busloc`, `busloc_vehicle`, `swiftly_bus_vehicle` |
| `--raw`                   | Download the file directly, without filtering or processing                                   |
| `--output [filepath]`     | Where to create the output file (default is `prediction-loc/output/[feed]-[datetime].json`)   |
| `--object-prefix [prefix]`| Specify a custom prefix for the key of the object to load from S3                             |

**Note:** route_id is matched exactly for the `bus` and `concentrate` feeds, but does substring matching for all others. For example, `--route Green` will include all Green Line branches, and `--route Worcester` will still match route_id `CR-Worcester`.

### Troubleshooting

If the `aws` command cannot be found, you will need to add it to your `PATH`:

1. Find the location of the install by running the command `python3 -c 'import awscli; print(awscli)'`
  - It should print something similar to  `<module 'awscli' from '/Users/localuser/Library/Python/3.6/lib/python/site-packages/awscli/__init__.py'\>`
2. Cut off the end of the filepath, starting with `lib`, and add `/bin` to get a path that looks like `/Users/localuser/Library/Python/3.6/bin`
3. Add that to your `PATH` by opening (or creating) `~/.bash_profile` and adding `export PATH=$PATH:/Users/localuser/Library/Python/3.6/bin` (using the path you created in step 2)
4. Open a new terminal window or run `source ~/.bash_profile` to apply your changes
