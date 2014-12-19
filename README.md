# DS-toolkit features (in no particular order)
* database profiling
* implicit primary key detection
* implicit foreign key detection
* database metamodel visualization
* deep database search (Elastic Search over each distinct value, of each column, of each table)
* sequence mining

# install virtualenvwrapper
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

# Profiling
First, create a `config.ini` file by copying the `template_config.ini` file and filling in the empty fields. [subjectdb] represents the database you will be profiling, [metadb] represents the database where profiling results will be stored and [es] containts Elastic Search  options.

* `workon dev3.4`
* `python profiler/profiler.py`

# Implicit primary key detection
* `python profiler/primarykey_detection.py -i <db_host> -u <db_user> -p <db_passwd> -c <db_catalog>`

# Implicit foreign key detection
* `python profiler/foreignkey_detection.py -i <db_host> -u <db_user> -p <db_passwd> -c <db_catalog>`

# Database metamodel visualization
