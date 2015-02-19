# DS-toolkit features (in no particular order)
* database profiling
* implicit primary key detection
* implicit foreign key detection
* database metamodel visualization
* deep database search (Elastic Search over each distinct value, of each column, of each table)
* sequence mining
* general utilities (csv2sql, htmltable2csv, mysql2csv, etc.)
* SQL to REST API to dynamically expose database objects via HTTP

# install virtualenvwrapper and setup two virtual environments
1. sudo apt-get install python-pip
2. sudo pip install virtualenv
3. mkdir ~/.virtualenvs
4. sudo pip install virtualenvwrapper
5. echo 'export WORKON_HOME=~/.virtualenvs'
6. echo '. /usr/local/bin/virtualenvwrapper.sh'
7. mkvirtualenv dev2.7 --python=\`which python2.7\`
8. mkvirtualenv dev3.4 --python=\`which python3.4\`

# install dependencies (in both virtual environments)
* pip install tornado
* pip install flask
* pip install flask_sqlalchemy
* pip install flask_restless
* pip install pymssql
* pip install sqlalchemy
* pip install pymssql
* pip install elasticsearch
* pip install requests

# Showcase
The show case will detail how to profile a database, visualize the metamodel, index an Elastic Search instance for deep database search as well as expose the database via HTTP.

Let's get some test data from [launchpad](https://launchpad.net/test-db/employees-db-1/1.0.6/+download/employees_db-full-1.0.6.tar.bz2) so that we have a predictable data set to work from.

TBD...

# Profiling
First, create a `config.ini` file by copying the `template_config.ini` file and filling in the empty fields.
* [subjectdb] the database you will be profiling,
* [metadb] the database where profiling results will be stored and
* [es] Elastic Search options.

Start the profiler from the dev3.4 virtual environment created earlier
* `workon dev3.4`
* `python profiler/profiler.py`

# Implicit primary key detection
* `python profiler/primarykey_detection.py -i <db_host> -u <db_user> -p <db_passwd> -c <db_catalog>`

# Implicit foreign key detection
* `python profiler/foreignkey_detection.py -i <db_host> -u <db_user> -p <db_passwd> -c <db_catalog>`
