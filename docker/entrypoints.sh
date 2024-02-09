#!/bin/bash
# vim:sw=4:ts=4:et:ai:ci:sr:nu:syntax=sh
##############################################################
# Usage ( * = optional ):                                    #
# ./script.sh *<db-address> *<db-port> *<username> *<password> #
##############################################################

mongoimport --drop --host mongo --port 27017 --db test --collection weather --file /weather/data.json --authenticationDatabase admin --username root --password example 
