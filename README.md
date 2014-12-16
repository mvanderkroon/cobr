
# install virtualenvwrapper
sudo apt-get install python-pip
sudo pip install virtualenv
mkdir ~/.virtualenvs
sudo pip install virtualenvwrapper

echo 'export WORKON_HOME=~/.virtualenvs'
echo '. /usr/local/bin/virtualenvwrapper.sh'

mkvirtualenv dev2.7 --python=`which python2.7`
mkvirtualenv dev3.4 --python=`which python3.4`

#install dependencies
pip install tornado
pip install flask
pip install flask_sqlalchemy
pip install flask_restless
pip install pymssql

pip install sqlalchemy
pip install pymssql
pip install elasticsearch
pip install requests