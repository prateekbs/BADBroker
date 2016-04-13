#!/bin/bash
while :
do
    python3 loadDDL.py
    python3 BADPublisher.py 'EmergencyShelters'
    python3 BADPublisher.py 'EmergencyReports'
    python3 BADPublisher.py 'UserLocations'
    sleep 1
done
