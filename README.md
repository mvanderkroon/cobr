
# install virtualenvwrapper
1. sudo apt-get install python-pip
2. sudo pip install virtualenv
3. mkdir ~/.virtualenvs
4. sudo pip install virtualenvwrapper
5. echo 'export WORKON_HOME=~/.virtualenvs'
6. echo '. /usr/local/bin/virtualenvwrapper.sh'
7. mkvirtualenv dev2.7 --python=`which python2.7`
8. mkvirtualenv dev3.4 --python=`which python3.4`

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