#!/bin/bash 
nohup python3 api_insert_medical_insurance_payments.py  insurance.log 2>&1 &
nohup python3 api_insert_pension_payments.py pension.log 2>&1 &
netstat -ntlp | grep "500"
