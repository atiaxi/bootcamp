#!/bin/bash

python setup.py sdist
scp -r dist/Capstone_KARJ-1.0.tar.gz root@workstation:/tmp
ssh root@workstation "cd /var/www;tar -zxvf /tmp/Capstone_KARJ-1.0.tar.gz;service apache2 restart"
