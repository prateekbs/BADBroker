#!/bin/bash
while :
do
    python3 loadDDL.py
    python3 BADPublisher.py 'EmergencyReports'
    sleep 1
done
